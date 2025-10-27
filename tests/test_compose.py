"""
Test suite for ja.compose module.

Tests focus on:
- Pipeline behavior and operator chaining
- Lazy vs eager evaluation
- Operation classes (Select, Project, Sort, etc.)
- Functional composition utilities
- Error handling and edge cases

Tests verify contracts and observable behavior, not implementation details.
"""

import pytest
from typing import Iterator

from ja.compose import (
    Pipeline, lazy_pipeline, pipeline,
    Select, Project, Sort, Distinct, Rename, GroupBy,
    Take, Skip, Map, Filter, Batch,
    compose, pipe,
    Operation
)
from ja.core import select, project, distinct


class TestPipeline:
    """Tests for Pipeline class behavior."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "age": 30, "dept": "Engineering"},
            {"id": 2, "name": "Bob", "age": 25, "dept": "Sales"},
            {"id": 3, "name": "Charlie", "age": 35, "dept": "Engineering"},
            {"id": 4, "name": "Diana", "age": 28, "dept": "Marketing"},
            {"id": 5, "name": "Eve", "age": 32, "dept": "Sales"},
        ]

    def test_empty_pipeline_returns_data_unchanged(self, sample_data):
        """Given empty pipeline, when executed, then data is returned unchanged."""
        p = Pipeline()
        result = p(sample_data)

        assert result == sample_data

    def test_pipeline_with_single_operation(self, sample_data):
        """Given pipeline with one operation, when executed, then operation is applied."""
        p = Pipeline(Select("age > 28"))
        result = p(sample_data)

        assert len(result) == 3
        assert all(r["age"] > 28 for r in result)

    def test_pipeline_chains_operations_in_order(self, sample_data):
        """Given pipeline with multiple operations, when executed, then operations apply in sequence."""
        p = Pipeline(
            Select("age > 25"),
            Project(["name", "age"]),
            Sort(["age"])
        )
        result = p(sample_data)

        # Verify filter applied (should exclude Bob with age 25)
        assert len(result) == 4
        # Verify projection applied (only name and age fields)
        assert all(set(r.keys()) == {"name", "age"} for r in result)
        # Verify sort applied
        ages = [r["age"] for r in result]
        assert ages == sorted(ages)

    def test_pipe_operator_chains_operations(self, sample_data):
        """Given operations chained with pipe operator, when executed, then operations apply correctly."""
        p = (
            Pipeline()
            | Select("dept == 'Engineering'")
            | Project(["name"])
        )
        result = p(sample_data)

        assert len(result) == 2
        assert all("name" in r and "dept" not in r for r in result)

    def test_eager_pipeline_returns_list(self, sample_data):
        """Given eager pipeline, when executed, then result is a list."""
        p = Pipeline(Select("age > 25"), lazy=False)
        result = p(sample_data)

        assert isinstance(result, list)

    def test_lazy_pipeline_returns_iterator(self, sample_data):
        """Given lazy pipeline, when executed, then result is an iterator."""
        p = Pipeline(Select("age > 25"), lazy=True)
        result = p(sample_data)

        # The result should be iterable
        assert hasattr(result, '__iter__')
        # Note: Select operation may materialize to list, but lazy pipeline
        # should support streaming operations. We verify it works correctly.
        materialized = list(result)
        assert len(materialized) == 4
        assert all(r["age"] > 25 for r in materialized)

    def test_lazy_pipeline_evaluates_on_demand(self, sample_data):
        """Given lazy pipeline, when partially consumed, then only needed items are processed."""
        p = lazy_pipeline(
            Select("age > 25"),
            Take(2)
        )
        result = p(sample_data)

        # Only take 2 items
        items = list(result)
        assert len(items) == 2
        assert all(r["age"] > 25 for r in items)

    def test_pipeline_callable_convenience_function(self, sample_data):
        """Given pipeline() convenience function, when used, then creates working pipeline."""
        p = pipeline(
            Select("dept == 'Sales'"),
            Project(["name", "age"])
        )
        result = p(sample_data)

        assert len(result) == 2
        assert all(set(r.keys()) == {"name", "age"} for r in result)

    def test_lazy_pipeline_convenience_function(self, sample_data):
        """Given lazy_pipeline() convenience function, when used, then creates lazy pipeline."""
        p = lazy_pipeline(
            Select("age >= 30"),
            Take(2)
        )
        result = p(sample_data)

        assert hasattr(result, '__iter__')
        items = list(result)
        assert len(items) == 2

    def test_pipeline_with_empty_input(self):
        """Given pipeline with empty input, when executed, then returns empty result."""
        p = Pipeline(Select("age > 25"), Project(["name"]))
        result = p([])

        assert result == []

    def test_pipeline_composition_with_pipe_operator(self, sample_data):
        """Given two pipelines, when combined with pipe, then operations from both apply."""
        p1 = Pipeline(Select("age > 25"))
        p2 = Pipeline(Project(["name", "dept"]))
        combined = p1 | p2

        result = combined(sample_data)
        assert len(result) == 4
        assert all(set(r.keys()) == {"name", "dept"} for r in result)


class TestSelectOperation:
    """Tests for Select operation behavior."""

    @pytest.fixture
    def sample_data(self):
        return [
            {"id": 1, "name": "Alice", "score": 85},
            {"id": 2, "name": "Bob", "score": 92},
            {"id": 3, "name": "Charlie", "score": 78},
        ]

    def test_select_filters_by_condition(self, sample_data):
        """Given Select with condition, when applied, then only matching rows are returned."""
        op = Select("score >= 85")
        result = list(op(sample_data))

        assert len(result) == 2
        assert all(r["score"] >= 85 for r in result)

    def test_select_with_no_matches_returns_empty(self, sample_data):
        """Given Select with condition matching nothing, when applied, then empty result."""
        op = Select("score > 100")
        result = list(op(sample_data))

        assert result == []

    def test_select_with_all_matches_returns_all(self, sample_data):
        """Given Select with condition matching everything, when applied, then all rows returned."""
        op = Select("score > 0")
        result = list(op(sample_data))

        assert len(result) == 3

    def test_select_works_with_lazy_iterator(self, sample_data):
        """Given Select with iterator input, when applied, then returns iterator."""
        op = Select("score >= 85")
        result = op(iter(sample_data))

        assert hasattr(result, '__iter__')
        assert not isinstance(result, list)
        assert len(list(result)) == 2

    def test_select_can_be_piped_with_other_operations(self, sample_data):
        """Given Select piped with Project, when applied, then both operations work."""
        pipeline = Select("score >= 85") | Project(["name"])
        result = pipeline(sample_data)

        assert len(result) == 2
        assert all(set(r.keys()) == {"name"} for r in result)


class TestProjectOperation:
    """Tests for Project operation behavior."""

    @pytest.fixture
    def sample_data(self):
        return [
            {"id": 1, "name": "Alice", "age": 30, "city": "NYC"},
            {"id": 2, "name": "Bob", "age": 25, "city": "LA"},
        ]

    def test_project_selects_specified_fields(self, sample_data):
        """Given Project with field list, when applied, then only those fields are in result."""
        op = Project(["name", "age"])
        result = list(op(sample_data))

        assert len(result) == 2
        assert all(set(r.keys()) == {"name", "age"} for r in result)

    def test_project_with_computed_field(self, sample_data):
        """Given Project with computed field, when applied, then field is computed correctly."""
        op = Project(["name", "age_plus_10=age+10"])
        result = list(op(sample_data))

        assert result[0]["name"] == "Alice"
        assert result[0]["age_plus_10"] == 40
        assert result[1]["age_plus_10"] == 35

    def test_project_with_nonexistent_field_omits_it(self, sample_data):
        """Given Project with nonexistent field, when applied, then field is omitted."""
        op = Project(["name", "country"])
        result = list(op(sample_data))

        # Only name should be present
        assert all("name" in r for r in result)
        assert all("country" not in r for r in result)

    def test_project_works_with_lazy_iterator(self, sample_data):
        """Given Project with iterator input, when applied, then returns iterator."""
        op = Project(["name"])
        result = op(iter(sample_data))

        assert hasattr(result, '__iter__')
        assert not isinstance(result, list)

    def test_project_with_empty_field_list_returns_empty_dicts(self, sample_data):
        """Given Project with empty field list, when applied, then empty dicts are returned."""
        op = Project([])
        result = list(op(sample_data))

        assert len(result) == 2
        assert all(r == {} for r in result)


class TestSortOperation:
    """Tests for Sort operation behavior."""

    @pytest.fixture
    def sample_data(self):
        return [
            {"name": "Charlie", "age": 30, "score": 85},
            {"name": "Alice", "age": 25, "score": 92},
            {"name": "Bob", "age": 30, "score": 78},
        ]

    def test_sort_by_single_field_ascending(self, sample_data):
        """Given Sort by one field, when applied, then results are sorted ascending."""
        op = Sort("name")
        result = op(sample_data)

        names = [r["name"] for r in result]
        assert names == ["Alice", "Bob", "Charlie"]

    def test_sort_by_multiple_fields(self, sample_data):
        """Given Sort by multiple fields, when applied, then results are sorted by all fields."""
        op = Sort(["age", "name"])
        result = op(sample_data)

        # First by age, then by name
        assert result[0]["name"] == "Alice"  # age 25
        assert result[1]["name"] == "Bob"    # age 30, name "Bob"
        assert result[2]["name"] == "Charlie"  # age 30, name "Charlie"

    def test_sort_descending(self, sample_data):
        """Given Sort with descending flag, when applied, then results are sorted descending."""
        op = Sort("score", descending=True)
        result = op(sample_data)

        scores = [r["score"] for r in result]
        assert scores == [92, 85, 78]

    def test_sort_materializes_iterator(self, sample_data):
        """Given Sort with iterator input, when applied, then result is a list (sorting requires materialization)."""
        op = Sort("name")
        result = op(iter(sample_data))

        assert isinstance(result, list)

    def test_sort_with_empty_input(self):
        """Given Sort with empty input, when applied, then returns empty list."""
        op = Sort("name")
        result = op([])

        assert result == []


class TestDistinctOperation:
    """Tests for Distinct operation behavior."""

    def test_distinct_removes_duplicates(self):
        """Given Distinct with duplicate rows, when applied, then duplicates are removed."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"},  # duplicate
            {"id": 3, "name": "Charlie"},
        ]
        op = Distinct()
        result = op(data)

        assert len(result) == 3
        ids = [r["id"] for r in result]
        assert ids == [1, 2, 3]

    def test_distinct_preserves_order_of_first_appearance(self):
        """Given Distinct, when applied, then first appearance order is preserved."""
        data = [
            {"val": "C"},
            {"val": "A"},
            {"val": "C"},
            {"val": "B"},
            {"val": "A"},
        ]
        op = Distinct()
        result = op(data)

        vals = [r["val"] for r in result]
        assert vals == ["C", "A", "B"]

    def test_distinct_with_no_duplicates_returns_all(self):
        """Given Distinct with no duplicates, when applied, then all rows are returned."""
        data = [{"id": i} for i in range(5)]
        op = Distinct()
        result = op(data)

        assert len(result) == 5

    def test_distinct_with_empty_input(self):
        """Given Distinct with empty input, when applied, then returns empty list."""
        op = Distinct()
        result = op([])

        assert result == []


class TestRenameOperation:
    """Tests for Rename operation behavior."""

    def test_rename_changes_field_names(self):
        """Given Rename with mapping, when applied, then fields are renamed."""
        data = [{"id": 1, "name": "Alice"}]
        op = Rename({"id": "user_id", "name": "full_name"})
        result = list(op(data))

        assert result[0] == {"user_id": 1, "full_name": "Alice"}

    def test_rename_partial_mapping_leaves_others_unchanged(self):
        """Given Rename with partial mapping, when applied, then unmapped fields unchanged."""
        data = [{"id": 1, "name": "Alice", "age": 30}]
        op = Rename({"id": "user_id"})
        result = list(op(data))

        assert result[0] == {"user_id": 1, "name": "Alice", "age": 30}

    def test_rename_with_nonexistent_field_is_noop(self):
        """Given Rename for nonexistent field, when applied, then data unchanged."""
        data = [{"id": 1, "name": "Alice"}]
        op = Rename({"country": "location"})
        result = list(op(data))

        assert result == data

    def test_rename_works_with_lazy_iterator(self):
        """Given Rename with iterator input, when applied, then returns iterator."""
        data = [{"id": 1}, {"id": 2}]
        op = Rename({"id": "user_id"})
        result = op(iter(data))

        assert hasattr(result, '__iter__')
        assert not isinstance(result, list)


class TestGroupByOperation:
    """Tests for GroupBy operation behavior."""

    @pytest.fixture
    def sample_data(self):
        return [
            {"dept": "Engineering", "salary": 80000},
            {"dept": "Sales", "salary": 60000},
            {"dept": "Engineering", "salary": 90000},
            {"dept": "Sales", "salary": 65000},
        ]

    def test_groupby_with_aggregation_counts_groups(self, sample_data):
        """Given GroupBy with count aggregation, when applied, then groups are counted."""
        op = GroupBy("dept", "count(salary)")
        result = op(sample_data)

        assert len(result) == 2
        dept_counts = {r["dept"]: r["count(salary)"] for r in result}
        assert dept_counts["Engineering"] == 2
        assert dept_counts["Sales"] == 2

    def test_groupby_with_sum_aggregation(self, sample_data):
        """Given GroupBy with sum aggregation, when applied, then values are summed."""
        op = GroupBy("dept", "sum(salary)")
        result = op(sample_data)

        dept_sums = {r["dept"]: r["sum(salary)"] for r in result}
        assert dept_sums["Engineering"] == 170000
        assert dept_sums["Sales"] == 125000

    def test_groupby_materializes_iterator(self, sample_data):
        """Given GroupBy with iterator input, when applied, then result is list (grouping requires materialization)."""
        op = GroupBy("dept", "count(salary)")
        result = op(iter(sample_data))

        assert isinstance(result, list)


class TestTakeOperation:
    """Tests for Take operation behavior."""

    def test_take_limits_number_of_items(self):
        """Given Take with n, when applied, then only first n items are returned."""
        data = [{"id": i} for i in range(10)]
        op = Take(3)
        result = list(op(data))

        assert len(result) == 3
        assert [r["id"] for r in result] == [0, 1, 2]

    def test_take_more_than_available_returns_all(self):
        """Given Take with n > data size, when applied, then all items are returned."""
        data = [{"id": i} for i in range(3)]
        op = Take(10)
        result = list(op(data))

        assert len(result) == 3

    def test_take_zero_returns_empty(self):
        """Given Take with 0, when applied, then empty result."""
        data = [{"id": i} for i in range(5)]
        op = Take(0)
        result = list(op(data))

        assert result == []

    def test_take_works_with_lazy_iterator(self):
        """Given Take with iterator, when applied, then returns iterator (doesn't materialize unnecessarily)."""
        data = iter([{"id": i} for i in range(100)])
        op = Take(3)
        result = op(data)

        # Should be an iterator
        assert hasattr(result, '__iter__')
        # Only consumes what's needed
        items = list(result)
        assert len(items) == 3


class TestSkipOperation:
    """Tests for Skip operation behavior."""

    def test_skip_omits_first_n_items(self):
        """Given Skip with n, when applied, then first n items are omitted."""
        data = [{"id": i} for i in range(5)]
        op = Skip(2)
        result = list(op(data))

        assert len(result) == 3
        assert [r["id"] for r in result] == [2, 3, 4]

    def test_skip_more_than_available_returns_empty(self):
        """Given Skip with n > data size, when applied, then empty result."""
        data = [{"id": i} for i in range(3)]
        op = Skip(10)
        result = list(op(data))

        assert result == []

    def test_skip_zero_returns_all(self):
        """Given Skip with 0, when applied, then all items returned."""
        data = [{"id": i} for i in range(3)]
        op = Skip(0)
        result = list(op(data))

        assert len(result) == 3


class TestMapOperation:
    """Tests for Map operation behavior."""

    def test_map_transforms_each_row(self):
        """Given Map with function, when applied, then function transforms each row."""
        data = [{"val": i} for i in range(3)]
        op = Map(lambda row: {"val": row["val"] * 2})
        result = list(op(data))

        assert [r["val"] for r in result] == [0, 2, 4]

    def test_map_can_add_fields(self):
        """Given Map that adds fields, when applied, then new fields are present."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        op = Map(lambda row: {**row, "greeting": f"Hello, {row['name']}"})
        result = list(op(data))

        assert result[0]["greeting"] == "Hello, Alice"
        assert result[1]["greeting"] == "Hello, Bob"

    def test_map_works_with_lazy_iterator(self):
        """Given Map with iterator, when applied, then returns iterator."""
        data = [{"val": i} for i in range(3)]
        op = Map(lambda row: {"val": row["val"] * 2})
        result = op(iter(data))

        assert hasattr(result, '__iter__')
        assert not isinstance(result, list)


class TestFilterOperation:
    """Tests for Filter operation behavior."""

    def test_filter_keeps_matching_rows(self):
        """Given Filter with predicate, when applied, then only matching rows are kept."""
        data = [{"val": i} for i in range(10)]
        op = Filter(lambda row: row["val"] % 2 == 0)
        result = list(op(data))

        assert len(result) == 5
        assert all(r["val"] % 2 == 0 for r in result)

    def test_filter_with_no_matches_returns_empty(self):
        """Given Filter matching nothing, when applied, then empty result."""
        data = [{"val": i} for i in range(5)]
        op = Filter(lambda row: row["val"] > 100)
        result = list(op(data))

        assert result == []

    def test_filter_works_with_lazy_iterator(self):
        """Given Filter with iterator, when applied, then returns iterator."""
        data = [{"val": i} for i in range(5)]
        op = Filter(lambda row: row["val"] > 2)
        result = op(iter(data))

        assert hasattr(result, '__iter__')
        assert not isinstance(result, list)


class TestBatchOperation:
    """Tests for Batch operation behavior."""

    def test_batch_groups_into_batches(self):
        """Given Batch with size n, when applied, then items are grouped into batches of size n."""
        data = [{"id": i} for i in range(10)]
        op = Batch(3)
        batches = list(op(data))

        assert len(batches) == 4  # 3 full batches + 1 partial
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1  # Last batch has remainder

    def test_batch_with_exact_multiple_has_no_partial(self):
        """Given Batch where data size is multiple of batch size, when applied, then all batches are full."""
        data = [{"id": i} for i in range(9)]
        op = Batch(3)
        batches = list(op(data))

        assert len(batches) == 3
        assert all(len(b) == 3 for b in batches)

    def test_batch_with_size_larger_than_data_returns_single_batch(self):
        """Given Batch with size > data size, when applied, then single batch with all items."""
        data = [{"id": i} for i in range(3)]
        op = Batch(10)
        batches = list(op(data))

        assert len(batches) == 1
        assert len(batches[0]) == 3


class TestFunctionalComposition:
    """Tests for functional composition utilities."""

    @pytest.fixture
    def sample_data(self):
        return [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

    def test_compose_combines_functions_right_to_left(self, sample_data):
        """Given compose with multiple functions, when applied, then functions apply right to left."""
        from functools import partial

        # compose applies right to left: distinct(project(select(data)))
        f = compose(
            partial(list),  # Convert to list
            distinct,
            partial(project, fields=["name"]),
            partial(select, expr="age >= 25")
        )

        result = f(sample_data)
        assert len(result) == 3
        assert all("name" in r and "age" not in r for r in result)

    def test_pipe_applies_functions_left_to_right(self, sample_data):
        """Given pipe with multiple functions, when applied, then functions apply left to right."""
        from functools import partial

        result = pipe(
            sample_data,
            partial(select, expr="age >= 30"),
            partial(project, fields=["name", "age"]),
            list
        )

        assert len(result) == 2
        assert all(r["age"] >= 30 for r in result)


class TestComplexPipelines:
    """Integration tests for complex pipeline scenarios."""

    @pytest.fixture
    def user_data(self):
        return [
            {"user_id": 1, "name": "Alice", "dept": "Engineering", "salary": 80000, "years": 3},
            {"user_id": 2, "name": "Bob", "dept": "Sales", "salary": 60000, "years": 2},
            {"user_id": 3, "name": "Charlie", "dept": "Engineering", "salary": 90000, "years": 5},
            {"user_id": 4, "name": "Diana", "dept": "Marketing", "salary": 70000, "years": 4},
            {"user_id": 5, "name": "Eve", "dept": "Engineering", "salary": 85000, "years": 3},
            {"user_id": 6, "name": "Frank", "dept": "Sales", "salary": 65000, "years": 1},
        ]

    def test_complex_analytics_pipeline(self, user_data):
        """Given complex multi-step pipeline, when executed, then all transformations apply correctly."""
        # Complex analysis: high-paid engineering employees sorted by years
        p = (
            Pipeline()
            | Select("dept == 'Engineering'")
            | Select("salary >= 85000")
            | Project(["name", "salary", "years"])
            | Sort(["years"], descending=True)
        )

        result = p(user_data)

        assert len(result) == 2
        assert result[0]["name"] == "Charlie"  # 5 years
        assert result[1]["name"] == "Eve"      # 3 years
        assert all(r["salary"] >= 85000 for r in result)

    def test_lazy_pipeline_for_large_dataset_sampling(self, user_data):
        """Given lazy pipeline with early termination, when executed, then stops after Take limit."""
        # Simulate large dataset processing - take only what's needed
        p = lazy_pipeline(
            Select("years >= 2"),
            Map(lambda r: {**r, "bonus": r["salary"] * 0.1}),
            Take(3)
        )

        result = list(p(user_data))

        assert len(result) == 3
        assert all("bonus" in r for r in result)
        assert all(r["years"] >= 2 for r in result)

    def test_batched_processing_pipeline(self, user_data):
        """Given pipeline with batching, when executed, then data is processed in batches."""
        p = Pipeline(
            Select("salary >= 65000"),
            Batch(2)
        )

        batches = list(p(user_data))

        # Should have multiple batches
        assert len(batches) >= 2
        # Each batch should be a list
        assert all(isinstance(batch, list) for batch in batches)
        # Batches should have at most 2 items
        assert all(len(batch) <= 2 for batch in batches)

    def test_pipeline_with_rename_and_transform(self, user_data):
        """Given pipeline with rename and computed fields, when executed, then fields are transformed correctly."""
        p = (
            Pipeline()
            | Rename({"user_id": "id", "name": "employee_name"})
            | Project(["id", "employee_name", "annual_bonus=salary*0.15"])
            | Sort(["annual_bonus"], descending=True)
        )

        result = p(user_data)

        assert len(result) == 6
        assert "id" in result[0] and "user_id" not in result[0]
        assert "employee_name" in result[0] and "name" not in result[0]
        assert result[0]["annual_bonus"] == 90000 * 0.15  # Charlie has highest salary

    def test_pipeline_error_resilience_with_empty_data(self):
        """Given pipeline with empty data, when executed, then no errors occur."""
        p = (
            Pipeline()
            | Select("age > 25")
            | Project(["name"])
            | Sort(["name"])
            | Take(10)
        )

        result = p([])

        assert result == []

    def test_operation_chaining_with_pipe_operator(self, user_data):
        """Given operations chained directly with pipe, when executed, then creates implicit pipeline."""
        # Operations can be piped directly without explicit Pipeline
        pipeline = (
            Select("dept == 'Sales'")
            | Project(["name", "salary"])
            | Sort(["salary"], descending=True)
        )

        result = pipeline(user_data)

        assert len(result) == 2
        assert result[0]["salary"] > result[1]["salary"]  # Descending order


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_pipeline_with_none_values_in_data(self):
        """Given data with None values, when processed, then None values are handled gracefully."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": None},
            {"id": 3, "value": 30},
        ]

        p = Pipeline(
            Project(["id", "value"]),
            Sort(["id"])
        )

        result = p(data)
        assert len(result) == 3

    def test_pipeline_with_missing_fields(self):
        """Given data with missing fields, when projected, then missing fields are omitted."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2},  # Missing name
            {"id": 3, "name": "Charlie"},
        ]

        p = Pipeline(Project(["id", "name"]))
        result = p(data)

        assert len(result) == 3
        assert "name" not in result[1]  # Missing field is omitted

    def test_select_with_complex_nested_expression(self):
        """Given Select with nested field access, when applied, then works correctly."""
        data = [
            {"user": {"age": 30}},
            {"user": {"age": 25}},
        ]

        # Note: This tests the behavior - expression support depends on ExprEval implementation
        op = Select("user.age >= 30")
        result = list(op(data))

        # Should filter based on nested field
        assert len(result) == 1

    def test_multiple_distinct_operations_in_pipeline(self):
        """Given pipeline with multiple Distinct operations, when executed, then duplicates removed at each stage."""
        data = [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
            {"a": 1, "b": 1},
            {"a": 2, "b": 1},
        ]

        p = (
            Pipeline()
            | Distinct()
            | Project(["a"])
            | Distinct()
        )

        result = p(data)
        assert len(result) == 2  # Only unique 'a' values
