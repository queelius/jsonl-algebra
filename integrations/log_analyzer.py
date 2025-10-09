#!/usr/bin/env python3
"""Real-time log analyzer using ja - showcases streaming and composability.

This integration demonstrates:
- Streaming processing of large log files
- Composable pipeline operations
- Real-time alerting and aggregation
- Sliding window analysis

Usage:
    # Analyze nginx logs in real-time
    tail -f /var/log/nginx/access.log | python log_analyzer.py

    # Analyze historical logs
    cat logs/*.jsonl | python log_analyzer.py --window 10

    # With custom alert thresholds
    python log_analyzer.py --error-threshold 10 --slow-threshold 2000 < app.log
"""

import sys
import json
import argparse
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Iterator, Optional
import time

# Import ja components
from ja import (
    Pipeline, lazy_pipeline,
    Select, Project, GroupBy,
    Map, Filter, Take, Batch,
    select, project, groupby_agg
)
from ja.expr import ExprEval


class LogAnalyzer:
    """Real-time log analysis with sliding windows and alerting."""

    def __init__(self,
                 window_minutes: int = 5,
                 error_threshold: int = 5,
                 slow_threshold: int = 1000,
                 batch_size: int = 100):
        """Initialize the log analyzer.

        Args:
            window_minutes: Size of sliding window in minutes
            error_threshold: Alert if errors exceed this count
            slow_threshold: Alert if response time exceeds this (ms)
            batch_size: Process logs in batches of this size
        """
        self.window = timedelta(minutes=window_minutes)
        self.error_threshold = error_threshold
        self.slow_threshold = slow_threshold
        self.batch_size = batch_size
        self.buffer = deque()
        self.stats = {
            'total_processed': 0,
            'errors_in_window': 0,
            'avg_response_time': 0,
            'requests_per_second': 0,
        }

    def parse_log_line(self, line: str) -> Optional[Dict]:
        """Parse a log line into structured data.

        Supports both JSON and common log formats.
        """
        line = line.strip()
        if not line:
            return None

        try:
            # Try JSON first
            return json.loads(line)
        except json.JSONDecodeError:
            # Try to parse common log format
            # Example: 127.0.0.1 - - [01/Jan/2024:12:00:00 +0000] "GET /api/users HTTP/1.1" 200 1234 0.123
            parts = line.split()
            if len(parts) >= 10:
                return {
                    'ip': parts[0],
                    'timestamp': datetime.now().isoformat(),
                    'method': parts[5].strip('"'),
                    'path': parts[6],
                    'status': int(parts[8]) if parts[8].isdigit() else 0,
                    'size': int(parts[9]) if parts[9].isdigit() else 0,
                    'response_time': float(parts[10]) if len(parts) > 10 else 0,
                    'level': 'ERROR' if parts[8].startswith('5') else 'INFO',
                }
        return None

    def enrich_log(self, log: Dict) -> Dict:
        """Enrich log entry with computed fields."""
        # Add timestamp if missing
        if 'timestamp' not in log:
            log['timestamp'] = datetime.now().isoformat()

        # Parse timestamp
        try:
            log['_timestamp'] = datetime.fromisoformat(log['timestamp'].replace('Z', '+00:00'))
        except:
            log['_timestamp'] = datetime.now()

        # Classify response status
        status = log.get('status', 0)
        if status >= 500:
            log['status_class'] = 'error'
        elif status >= 400:
            log['status_class'] = 'client_error'
        elif status >= 300:
            log['status_class'] = 'redirect'
        elif status >= 200:
            log['status_class'] = 'success'
        else:
            log['status_class'] = 'unknown'

        # Flag slow requests
        response_time = log.get('response_time', 0)
        log['is_slow'] = response_time > self.slow_threshold

        return log

    def update_sliding_window(self, logs: List[Dict]) -> List[Dict]:
        """Update sliding window buffer and return current window."""
        now = datetime.now()
        cutoff = now - self.window

        # Add new logs to buffer
        self.buffer.extend(logs)

        # Remove old entries
        while self.buffer and self.buffer[0].get('_timestamp', now) < cutoff:
            self.buffer.popleft()

        return list(self.buffer)

    def analyze_window(self, window_logs: List[Dict]) -> Dict:
        """Analyze logs in current window."""
        if not window_logs:
            return {}

        # Create analysis pipeline
        analysis_pipeline = Pipeline(
            # Count errors
            Select("status >= 500"),
        )

        errors = list(analysis_pipeline(window_logs))
        error_count = len(errors)

        # Calculate metrics using ja operations
        # Response time statistics
        response_times = [log.get('response_time', 0) for log in window_logs]
        avg_response = sum(response_times) / len(response_times) if response_times else 0

        # Requests per second
        time_range = (window_logs[-1]['_timestamp'] - window_logs[0]['_timestamp']).total_seconds()
        rps = len(window_logs) / time_range if time_range > 0 else 0

        # Group by endpoint
        endpoint_stats = groupby_agg(
            window_logs,
            'path',
            'count=count,avg_time=avg(response_time),errors=sum(status>=500)'
        )

        # Find slow endpoints
        slow_endpoints = select(endpoint_stats, 'avg_time > {}'.format(self.slow_threshold))

        # Group by status class
        status_distribution = groupby_agg(window_logs, 'status_class', 'count=count')

        return {
            'total_requests': len(window_logs),
            'error_count': error_count,
            'error_rate': error_count / len(window_logs) if window_logs else 0,
            'avg_response_time': avg_response,
            'requests_per_second': rps,
            'slow_endpoints': slow_endpoints,
            'status_distribution': status_distribution,
            'top_errors': errors[:5] if errors else [],
        }

    def check_alerts(self, analysis: Dict) -> List[str]:
        """Check for alert conditions."""
        alerts = []

        # Error threshold alert
        if analysis.get('error_count', 0) > self.error_threshold:
            alerts.append(
                f"ERROR SPIKE: {analysis['error_count']} errors in window "
                f"(threshold: {self.error_threshold})"
            )

        # Slow response alert
        if analysis.get('avg_response_time', 0) > self.slow_threshold:
            alerts.append(
                f"SLOW RESPONSE: Average response time {analysis['avg_response_time']:.2f}ms "
                f"(threshold: {self.slow_threshold}ms)"
            )

        # High error rate alert
        error_rate = analysis.get('error_rate', 0)
        if error_rate > 0.05:  # 5% error rate
            alerts.append(f"HIGH ERROR RATE: {error_rate:.2%} of requests failing")

        # Slow endpoints alert
        slow_endpoints = analysis.get('slow_endpoints', [])
        if slow_endpoints:
            endpoints = ', '.join(ep['path'] for ep in slow_endpoints[:3])
            alerts.append(f"SLOW ENDPOINTS: {endpoints}")

        return alerts

    def print_dashboard(self, analysis: Dict, alerts: List[str]):
        """Print a real-time dashboard."""
        # Clear screen for dashboard effect (optional)
        # print("\033[2J\033[H")  # Uncomment for clear screen

        print("\n" + "=" * 60)
        print(f"Log Analysis Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # Metrics
        print(f"\nWindow: {self.window.total_seconds() / 60:.0f} minutes")
        print(f"Total Requests: {analysis.get('total_requests', 0)}")
        print(f"Requests/sec: {analysis.get('requests_per_second', 0):.2f}")
        print(f"Avg Response Time: {analysis.get('avg_response_time', 0):.2f}ms")
        print(f"Error Count: {analysis.get('error_count', 0)}")
        print(f"Error Rate: {analysis.get('error_rate', 0):.2%}")

        # Status distribution
        if analysis.get('status_distribution'):
            print("\nStatus Distribution:")
            for status in analysis['status_distribution']:
                print(f"  {status['status_class']}: {status['count']}")

        # Alerts
        if alerts:
            print(f"\n⚠️  ALERTS ({len(alerts)}):")
            for alert in alerts:
                print(f"  • {alert}")
        else:
            print("\n✓ All systems operational")

        # Top errors
        if analysis.get('top_errors'):
            print("\nRecent Errors:")
            for error in analysis['top_errors'][:3]:
                print(f"  • [{error.get('timestamp')}] {error.get('path')} - {error.get('status')}")

        print("=" * 60)

    def process_stream(self, stream: Iterator[str]):
        """Process a stream of log lines."""
        # Create processing pipeline
        process_pipeline = lazy_pipeline(
            Map(self.parse_log_line),
            Filter(lambda x: x is not None),
            Map(self.enrich_log),
            Batch(self.batch_size),
        )

        # Process batches
        for batch in process_pipeline(stream):
            self.stats['total_processed'] += len(batch)

            # Update sliding window
            window_logs = self.update_sliding_window(batch)

            # Analyze window
            if window_logs:
                analysis = self.analyze_window(window_logs)

                # Check for alerts
                alerts = self.check_alerts(analysis)

                # Update dashboard
                self.print_dashboard(analysis, alerts)

                # Log alerts to stderr for monitoring systems
                for alert in alerts:
                    print(f"[ALERT] {alert}", file=sys.stderr)

            # Small delay to prevent CPU spinning on empty input
            time.sleep(0.1)

    def run(self):
        """Run the analyzer on stdin."""
        try:
            self.process_stream(sys.stdin)
        except KeyboardInterrupt:
            print(f"\n\nShutting down. Processed {self.stats['total_processed']} logs.")
            sys.exit(0)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Real-time log analyzer using ja',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--window',
        type=int,
        default=5,
        help='Sliding window size in minutes (default: 5)'
    )

    parser.add_argument(
        '--error-threshold',
        type=int,
        default=5,
        help='Alert if errors exceed this count (default: 5)'
    )

    parser.add_argument(
        '--slow-threshold',
        type=int,
        default=1000,
        help='Alert if response time exceeds this in ms (default: 1000)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Process logs in batches of this size (default: 100)'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Generate test data for demonstration'
    )

    args = parser.parse_args()

    if args.test:
        # Generate test data
        import random
        for i in range(1000):
            log = {
                'timestamp': datetime.now().isoformat(),
                'path': random.choice(['/api/users', '/api/orders', '/api/products', '/health']),
                'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
                'status': random.choice([200, 200, 200, 200, 201, 301, 400, 404, 500, 503]),
                'response_time': random.gauss(100, 50) if random.random() > 0.1 else random.gauss(2000, 500),
                'ip': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            }
            print(json.dumps(log))
            time.sleep(random.uniform(0.01, 0.1))
        return

    # Run analyzer
    analyzer = LogAnalyzer(
        window_minutes=args.window,
        error_threshold=args.error_threshold,
        slow_threshold=args.slow_threshold,
        batch_size=args.batch_size,
    )
    analyzer.run()


if __name__ == '__main__':
    main()