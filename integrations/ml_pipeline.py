#!/usr/bin/env python3
"""Machine Learning feature engineering pipeline using ja.

This integration demonstrates:
- Feature engineering with ja transformations
- Data preprocessing and normalization
- Train/test split with JSONL persistence
- Integration with scikit-learn
- Model evaluation pipeline

Usage:
    # Feature engineering
    python ml_pipeline.py prepare data.jsonl --target price

    # Train model
    python ml_pipeline.py train train.jsonl --model rf

    # Evaluate model
    python ml_pipeline.py evaluate test.jsonl --model model.pkl

    # Full pipeline
    python ml_pipeline.py pipeline data.jsonl --target price --model rf
"""

import sys
import json
import pickle
import argparse
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime

from ja import (
    select, project, groupby_agg, sort_by,
    Pipeline, Select, Project, Map, Filter,
    compose, pipe
)
from ja.commands import read_jsonl
from ja.expr import ExprEval


class FeatureEngineer:
    """Feature engineering using ja operations."""

    def __init__(self, target_field: str = 'target'):
        """Initialize feature engineer.

        Args:
            target_field: Name of the target variable field
        """
        self.target_field = target_field
        self.feature_metadata = {}
        self.parser = ExprEval()

    def engineer_features(self, data: List[Dict]) -> Tuple[List[Dict], Dict]:
        """Engineer features from raw data.

        Args:
            data: Raw data records

        Returns:
            Tuple of (engineered features, metadata)
        """
        # Step 1: Basic feature extraction
        print("Step 1: Extracting basic features...")
        basic_features = self._extract_basic_features(data)

        # Step 2: Aggregate features
        print("Step 2: Creating aggregate features...")
        agg_features = self._create_aggregate_features(basic_features)

        # Step 3: Interaction features
        print("Step 3: Creating interaction features...")
        interaction_features = self._create_interaction_features(agg_features)

        # Step 4: Time-based features (if applicable)
        print("Step 4: Creating time features...")
        time_features = self._create_time_features(interaction_features)

        # Step 5: Encode categorical variables
        print("Step 5: Encoding categorical variables...")
        encoded_features = self._encode_categoricals(time_features)

        # Store metadata
        self.feature_metadata = {
            'num_features': len(encoded_features[0]) if encoded_features else 0,
            'feature_names': list(encoded_features[0].keys()) if encoded_features else [],
            'num_samples': len(encoded_features),
            'engineering_timestamp': datetime.now().isoformat(),
        }

        return encoded_features, self.feature_metadata

    def _extract_basic_features(self, data: List[Dict]) -> List[Dict]:
        """Extract basic features from raw data."""
        # Identify numeric and categorical fields
        sample = data[0] if data else {}
        numeric_fields = []
        categorical_fields = []

        for field, value in sample.items():
            if field == self.target_field:
                continue
            if isinstance(value, (int, float)):
                numeric_fields.append(field)
            elif isinstance(value, str):
                categorical_fields.append(field)

        # Project relevant fields and compute derived features
        projection_specs = numeric_fields + categorical_fields

        # Add computed features
        projection_specs.extend([
            # Example computed features - adjust based on your data
            f"has_{field}={field} != null" for field in numeric_fields[:3]
        ])

        if numeric_fields:
            # Add ratios for numeric fields
            if len(numeric_fields) >= 2:
                projection_specs.append(
                    f"ratio_{numeric_fields[0]}_{numeric_fields[1]}="
                    f"{numeric_fields[0]}/{numeric_fields[1]}"
                )

        # Always include target if present
        if any(self.target_field in row for row in data):
            projection_specs.append(self.target_field)

        return project(data, projection_specs)

    def _create_aggregate_features(self, data: List[Dict]) -> List[Dict]:
        """Create aggregate features by grouping."""
        # Check if there are categorical fields to group by
        sample = data[0] if data else {}
        categorical_fields = [
            field for field, value in sample.items()
            if isinstance(value, str) and field != self.target_field
        ]

        if not categorical_fields:
            return data

        # Group by first categorical field and compute aggregates
        group_field = categorical_fields[0]
        numeric_fields = [
            field for field, value in sample.items()
            if isinstance(value, (int, float)) and field != self.target_field
        ]

        if not numeric_fields:
            return data

        # Compute group statistics
        agg_spec = []
        for field in numeric_fields[:3]:  # Limit to first 3 numeric fields
            agg_spec.append(f"mean_{field}=avg({field})")
            agg_spec.append(f"std_{field}=std({field})")

        if agg_spec:
            grouped = groupby_agg(data, group_field, ",".join(agg_spec))

            # Join back to original data
            from ja import join
            enriched = data
            # Note: join implementation would need adjustment for this use case
            # For now, we'll simulate by adding group stats to each row

            group_stats = {row[group_field]: row for row in grouped}
            for row in enriched:
                group_key = row.get(group_field)
                if group_key in group_stats:
                    stats = group_stats[group_key]
                    for key, value in stats.items():
                        if key != group_field:
                            row[f"group_{key}"] = value

        return data

    def _create_interaction_features(self, data: List[Dict]) -> List[Dict]:
        """Create interaction features between fields."""
        if not data:
            return data

        sample = data[0]
        numeric_fields = [
            field for field, value in sample.items()
            if isinstance(value, (int, float)) and field != self.target_field
        ]

        # Create pairwise interactions for first few numeric fields
        interactions = []
        for i, field1 in enumerate(numeric_fields[:3]):
            for field2 in numeric_fields[i+1:4]:
                interactions.append(f"interact_{field1}_{field2}={field1}*{field2}")

        if interactions:
            # Apply interactions using project
            all_fields = list(sample.keys()) + interactions
            return project(data, all_fields)

        return data

    def _create_time_features(self, data: List[Dict]) -> List[Dict]:
        """Create time-based features if timestamp fields exist."""
        if not data:
            return data

        sample = data[0]
        time_fields = [
            field for field, value in sample.items()
            if 'time' in field.lower() or 'date' in field.lower()
        ]

        if not time_fields:
            return data

        # Add time-based features
        for field in time_fields[:1]:  # Process first timestamp field
            try:
                # Parse and extract components
                enriched = []
                for row in data:
                    if field in row:
                        try:
                            dt = datetime.fromisoformat(str(row[field]).replace('Z', '+00:00'))
                            row[f"{field}_year"] = dt.year
                            row[f"{field}_month"] = dt.month
                            row[f"{field}_day"] = dt.day
                            row[f"{field}_hour"] = dt.hour
                            row[f"{field}_weekday"] = dt.weekday()
                        except:
                            pass
                    enriched.append(row)
                data = enriched
            except:
                pass

        return data

    def _encode_categoricals(self, data: List[Dict]) -> List[Dict]:
        """Encode categorical variables."""
        if not data:
            return data

        sample = data[0]
        categorical_fields = [
            field for field, value in sample.items()
            if isinstance(value, str) and field != self.target_field
        ]

        # One-hot encode categorical fields with low cardinality
        for field in categorical_fields:
            # Get unique values
            values = list(set(row.get(field) for row in data if row.get(field)))

            if len(values) <= 10:  # Only encode if cardinality is low
                # Create binary features
                for value in values:
                    safe_value = str(value).replace(' ', '_').replace('-', '_')
                    for row in data:
                        row[f"{field}_is_{safe_value}"] = 1 if row.get(field) == value else 0

                # Remove original categorical field
                for row in data:
                    row.pop(field, None)

        return data

    def prepare_ml_data(self, features: List[Dict]) -> Tuple[np.ndarray, np.ndarray]:
        """Convert features to numpy arrays for ML.

        Args:
            features: List of feature dictionaries

        Returns:
            Tuple of (X, y) numpy arrays
        """
        if not features:
            return np.array([]), np.array([])

        # Separate features and target
        X_data = []
        y_data = []

        # Get feature names (excluding target)
        feature_names = [
            key for key in features[0].keys()
            if key != self.target_field
        ]

        for row in features:
            # Extract features
            x_row = []
            for name in feature_names:
                value = row.get(name, 0)
                # Convert to numeric, default to 0
                if isinstance(value, (int, float)):
                    x_row.append(value)
                elif isinstance(value, bool):
                    x_row.append(1 if value else 0)
                else:
                    x_row.append(0)
            X_data.append(x_row)

            # Extract target
            target = row.get(self.target_field, 0)
            y_data.append(float(target) if target is not None else 0)

        return np.array(X_data), np.array(y_data)

    def save_features(self, features: List[Dict], filename: str):
        """Save engineered features to JSONL."""
        with open(filename, 'w') as f:
            for row in features:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        print(f"Saved {len(features)} feature records to {filename}")

    def load_features(self, filename: str) -> List[Dict]:
        """Load features from JSONL."""
        return list(read_jsonl(filename))


class MLPipeline:
    """Complete ML pipeline with ja integration."""

    def __init__(self, target_field: str = 'target'):
        self.target_field = target_field
        self.feature_engineer = FeatureEngineer(target_field)
        self.model = None
        self.scaler = None

    def prepare_data(self, input_file: str, train_ratio: float = 0.8):
        """Prepare data for ML training.

        Args:
            input_file: Input JSONL file
            train_ratio: Ratio of data for training

        Returns:
            Paths to train and test files
        """
        # Load data
        data = list(read_jsonl(input_file))
        print(f"Loaded {len(data)} records from {input_file}")

        # Engineer features
        features, metadata = self.feature_engineer.engineer_features(data)
        print(f"Engineered {len(metadata['feature_names'])} features")

        # Split data
        split_idx = int(len(features) * train_ratio)
        train_data = features[:split_idx]
        test_data = features[split_idx:]

        # Save splits
        self.feature_engineer.save_features(train_data, 'train.jsonl')
        self.feature_engineer.save_features(test_data, 'test.jsonl')

        # Save metadata
        with open('feature_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"Train set: {len(train_data)} samples")
        print(f"Test set: {len(test_data)} samples")

        return 'train.jsonl', 'test.jsonl'

    def train_model(self, train_file: str, model_type: str = 'rf'):
        """Train ML model on prepared data.

        Args:
            train_file: Training data file
            model_type: Type of model ('rf', 'gb', 'linear')
        """
        # Import sklearn components
        try:
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            from sklearn.linear_model import LinearRegression
            from sklearn.preprocessing import StandardScaler
            from sklearn.metrics import mean_squared_error, r2_score
        except ImportError:
            print("Error: scikit-learn is required. Install with: pip install scikit-learn")
            return

        # Load training data
        train_data = self.feature_engineer.load_features(train_file)
        X_train, y_train = self.feature_engineer.prepare_ml_data(train_data)

        print(f"Training on {X_train.shape[0]} samples with {X_train.shape[1]} features")

        # Scale features
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)

        # Select model
        if model_type == 'rf':
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        elif model_type == 'gb':
            self.model = GradientBoostingRegressor(n_estimators=100, random_state=42)
        else:
            self.model = LinearRegression()

        # Train model
        print(f"Training {model_type} model...")
        self.model.fit(X_train_scaled, y_train)

        # Evaluate on training set
        y_pred = self.model.predict(X_train_scaled)
        train_mse = mean_squared_error(y_train, y_pred)
        train_r2 = r2_score(y_train, y_pred)

        print(f"Training MSE: {train_mse:.4f}")
        print(f"Training R2: {train_r2:.4f}")

        # Save model and scaler
        with open('model.pkl', 'wb') as f:
            pickle.dump({'model': self.model, 'scaler': self.scaler}, f)
        print("Model saved to model.pkl")

        # Feature importance (if available)
        if hasattr(self.model, 'feature_importances_'):
            importances = self.model.feature_importances_
            feature_names = self.feature_engineer.feature_metadata.get('feature_names', [])
            if feature_names:
                importance_pairs = sorted(
                    zip(feature_names, importances),
                    key=lambda x: x[1],
                    reverse=True
                )
                print("\nTop 10 Important Features:")
                for name, imp in importance_pairs[:10]:
                    print(f"  {name}: {imp:.4f}")

    def evaluate_model(self, test_file: str, model_file: str = 'model.pkl'):
        """Evaluate model on test data.

        Args:
            test_file: Test data file
            model_file: Saved model file
        """
        try:
            from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
        except ImportError:
            print("Error: scikit-learn is required")
            return

        # Load model
        with open(model_file, 'rb') as f:
            saved = pickle.load(f)
            self.model = saved['model']
            self.scaler = saved['scaler']

        # Load test data
        test_data = self.feature_engineer.load_features(test_file)
        X_test, y_test = self.feature_engineer.prepare_ml_data(test_data)

        # Scale features
        X_test_scaled = self.scaler.transform(X_test)

        # Predict
        y_pred = self.model.predict(X_test_scaled)

        # Calculate metrics
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        print(f"\nTest Set Evaluation:")
        print(f"  MSE: {mse:.4f}")
        print(f"  MAE: {mae:.4f}")
        print(f"  R2 Score: {r2:.4f}")

        # Save predictions
        predictions = []
        for i, row in enumerate(test_data):
            pred_row = row.copy()
            pred_row['prediction'] = float(y_pred[i])
            pred_row['actual'] = float(y_test[i])
            pred_row['error'] = float(y_pred[i] - y_test[i])
            predictions.append(pred_row)

        # Analyze errors
        error_pipeline = Pipeline(
            Project(['error', 'actual', 'prediction']),
            Map(lambda x: {...x, 'abs_error': abs(x['error'])}),
            Sort('abs_error', descending=True)
        )

        worst_predictions = list(error_pipeline(predictions))[:10]

        print("\nWorst 10 Predictions:")
        for pred in worst_predictions:
            print(f"  Actual: {pred['actual']:.2f}, "
                  f"Predicted: {pred['prediction']:.2f}, "
                  f"Error: {pred['error']:.2f}")

        # Save predictions
        with open('predictions.jsonl', 'w') as f:
            for row in predictions:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        print(f"\nSaved {len(predictions)} predictions to predictions.jsonl")

    def run_full_pipeline(self, input_file: str, model_type: str = 'rf'):
        """Run complete ML pipeline."""
        print("=" * 60)
        print("Running Full ML Pipeline")
        print("=" * 60)

        # Step 1: Prepare data
        print("\n1. Preparing Data...")
        train_file, test_file = self.prepare_data(input_file)

        # Step 2: Train model
        print("\n2. Training Model...")
        self.train_model(train_file, model_type)

        # Step 3: Evaluate model
        print("\n3. Evaluating Model...")
        self.evaluate_model(test_file)

        print("\n" + "=" * 60)
        print("Pipeline Complete!")
        print("=" * 60)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='ML pipeline with ja feature engineering',
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Prepare command
    prepare_parser = subparsers.add_parser('prepare', help='Prepare data for ML')
    prepare_parser.add_argument('input', help='Input JSONL file')
    prepare_parser.add_argument('--target', default='target', help='Target field name')
    prepare_parser.add_argument('--split', type=float, default=0.8, help='Train split ratio')

    # Train command
    train_parser = subparsers.add_parser('train', help='Train ML model')
    train_parser.add_argument('input', help='Training data file')
    train_parser.add_argument('--model', choices=['rf', 'gb', 'linear'], default='rf',
                              help='Model type')
    train_parser.add_argument('--target', default='target', help='Target field name')

    # Evaluate command
    eval_parser = subparsers.add_parser('evaluate', help='Evaluate model')
    eval_parser.add_argument('input', help='Test data file')
    eval_parser.add_argument('--model', default='model.pkl', help='Model file')
    eval_parser.add_argument('--target', default='target', help='Target field name')

    # Pipeline command
    pipeline_parser = subparsers.add_parser('pipeline', help='Run full pipeline')
    pipeline_parser.add_argument('input', help='Input JSONL file')
    pipeline_parser.add_argument('--target', default='target', help='Target field name')
    pipeline_parser.add_argument('--model', choices=['rf', 'gb', 'linear'], default='rf',
                                 help='Model type')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Create pipeline
    ml_pipeline = MLPipeline(target_field=args.target if hasattr(args, 'target') else 'target')

    # Execute command
    if args.command == 'prepare':
        ml_pipeline.prepare_data(args.input, args.split)
    elif args.command == 'train':
        ml_pipeline.train_model(args.input, args.model)
    elif args.command == 'evaluate':
        ml_pipeline.evaluate_model(args.input, args.model)
    elif args.command == 'pipeline':
        ml_pipeline.run_full_pipeline(args.input, args.model)


if __name__ == '__main__':
    main()