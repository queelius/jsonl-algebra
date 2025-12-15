"""Tests for window functions module."""

import pytest
from ja.window import (
    row_number,
    rank,
    dense_rank,
    lag,
    lead,
    first_value,
    last_value,
    ntile,
    percent_rank,
    cume_dist,
)


class TestRowNumber:
    """Tests for row_number function."""

    def test_row_number_single_partition(self):
        """Row number assigns sequential numbers to all rows."""
        data = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
        result = row_number(data, order_by="name")

        assert result[0]["_row_number"] == 1  # Alice
        assert result[1]["_row_number"] == 2  # Bob
        assert result[2]["_row_number"] == 3  # Charlie

    def test_row_number_with_partition(self):
        """Row number restarts for each partition."""
        data = [
            {"dept": "A", "name": "Alice"},
            {"dept": "A", "name": "Bob"},
            {"dept": "B", "name": "Charlie"},
            {"dept": "B", "name": "Diana"},
        ]
        result = row_number(data, partition_by="dept", order_by="name")

        # Find by name since order might vary
        alice = next(r for r in result if r["name"] == "Alice")
        bob = next(r for r in result if r["name"] == "Bob")
        charlie = next(r for r in result if r["name"] == "Charlie")
        diana = next(r for r in result if r["name"] == "Diana")

        assert alice["_row_number"] == 1
        assert bob["_row_number"] == 2
        assert charlie["_row_number"] == 1
        assert diana["_row_number"] == 2

    def test_row_number_custom_output_field(self):
        """Can specify custom output field name."""
        data = [{"name": "Alice"}]
        result = row_number(data, output_field="rn")

        assert "rn" in result[0]
        assert result[0]["rn"] == 1

    def test_row_number_preserves_original_data(self):
        """Original fields are preserved."""
        data = [{"name": "Alice", "age": 30}]
        result = row_number(data)

        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30


class TestRank:
    """Tests for rank function (with gaps for ties)."""

    def test_rank_no_ties(self):
        """Without ties, rank equals row number."""
        data = [{"score": 100}, {"score": 90}, {"score": 80}]
        result = rank(data, order_by="score")

        # Ordered by score ascending by default
        assert result[2]["_rank"] == 1  # 80
        assert result[1]["_rank"] == 2  # 90
        assert result[0]["_rank"] == 3  # 100

    def test_rank_with_ties(self):
        """Ties get same rank, next rank skips."""
        data = [
            {"name": "A", "score": 100},
            {"name": "B", "score": 100},
            {"name": "C", "score": 90},
        ]
        result = rank(data, order_by="score")

        # Two 100s tie at rank 3 (highest when ascending)
        a = next(r for r in result if r["name"] == "A")
        b = next(r for r in result if r["name"] == "B")
        c = next(r for r in result if r["name"] == "C")

        assert a["_rank"] == b["_rank"]  # Ties
        assert c["_rank"] == 1  # 90 is first (lowest)
        assert a["_rank"] == 2  # 100s are rank 2 and 3


class TestDenseRank:
    """Tests for dense_rank function (no gaps for ties)."""

    def test_dense_rank_no_ties(self):
        """Without ties, dense rank equals row number."""
        data = [{"score": 100}, {"score": 90}, {"score": 80}]
        result = dense_rank(data, order_by="score")

        assert result[2]["_dense_rank"] == 1  # 80
        assert result[1]["_dense_rank"] == 2  # 90
        assert result[0]["_dense_rank"] == 3  # 100

    def test_dense_rank_with_ties(self):
        """Ties get same rank, next rank continues sequentially."""
        data = [
            {"name": "A", "score": 100},
            {"name": "B", "score": 100},
            {"name": "C", "score": 90},
            {"name": "D", "score": 80},
        ]
        result = dense_rank(data, order_by="score")

        a = next(r for r in result if r["name"] == "A")
        b = next(r for r in result if r["name"] == "B")
        c = next(r for r in result if r["name"] == "C")
        d = next(r for r in result if r["name"] == "D")

        assert d["_dense_rank"] == 1  # 80
        assert c["_dense_rank"] == 2  # 90
        assert a["_dense_rank"] == b["_dense_rank"] == 3  # 100s tie


class TestLag:
    """Tests for lag function."""

    def test_lag_default_offset(self):
        """Lag gets value from previous row."""
        data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}, {"id": 3, "value": "c"}]
        result = lag(data, field="value", order_by="id")

        assert result[0]["_lag_value"] is None  # No previous
        assert result[1]["_lag_value"] == "a"
        assert result[2]["_lag_value"] == "b"

    def test_lag_custom_offset(self):
        """Lag can look back multiple rows."""
        data = [{"id": 1, "v": 10}, {"id": 2, "v": 20}, {"id": 3, "v": 30}]
        result = lag(data, field="v", offset=2, order_by="id")

        assert result[0]["_lag_v"] is None
        assert result[1]["_lag_v"] is None
        assert result[2]["_lag_v"] == 10

    def test_lag_custom_default(self):
        """Lag can use custom default value."""
        data = [{"id": 1, "value": "a"}]
        result = lag(data, field="value", default="N/A", order_by="id")

        assert result[0]["_lag_value"] == "N/A"

    def test_lag_with_partition(self):
        """Lag respects partitions."""
        data = [
            {"dept": "A", "id": 1, "value": "a1"},
            {"dept": "A", "id": 2, "value": "a2"},
            {"dept": "B", "id": 1, "value": "b1"},
        ]
        result = lag(data, field="value", partition_by="dept", order_by="id")

        a1 = next(r for r in result if r["value"] == "a1")
        a2 = next(r for r in result if r["value"] == "a2")
        b1 = next(r for r in result if r["value"] == "b1")

        assert a1["_lag_value"] is None  # First in partition A
        assert a2["_lag_value"] == "a1"
        assert b1["_lag_value"] is None  # First in partition B


class TestLead:
    """Tests for lead function."""

    def test_lead_default_offset(self):
        """Lead gets value from next row."""
        data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}, {"id": 3, "value": "c"}]
        result = lead(data, field="value", order_by="id")

        assert result[0]["_lead_value"] == "b"
        assert result[1]["_lead_value"] == "c"
        assert result[2]["_lead_value"] is None  # No next

    def test_lead_custom_offset(self):
        """Lead can look forward multiple rows."""
        data = [{"id": 1, "v": 10}, {"id": 2, "v": 20}, {"id": 3, "v": 30}]
        result = lead(data, field="v", offset=2, order_by="id")

        assert result[0]["_lead_v"] == 30
        assert result[1]["_lead_v"] is None
        assert result[2]["_lead_v"] is None


class TestFirstValue:
    """Tests for first_value function."""

    def test_first_value_single_partition(self):
        """First value of entire dataset."""
        data = [{"id": 3, "name": "C"}, {"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        result = first_value(data, field="name", order_by="id")

        # All rows get first value (A, when ordered by id)
        for row in result:
            assert row["_first_name"] == "A"

    def test_first_value_with_partition(self):
        """First value per partition."""
        data = [
            {"dept": "X", "id": 2, "name": "X2"},
            {"dept": "X", "id": 1, "name": "X1"},
            {"dept": "Y", "id": 1, "name": "Y1"},
        ]
        result = first_value(data, field="name", partition_by="dept", order_by="id")

        x2 = next(r for r in result if r["name"] == "X2")
        x1 = next(r for r in result if r["name"] == "X1")
        y1 = next(r for r in result if r["name"] == "Y1")

        assert x1["_first_name"] == "X1"  # First in X partition
        assert x2["_first_name"] == "X1"
        assert y1["_first_name"] == "Y1"  # First in Y partition


class TestLastValue:
    """Tests for last_value function."""

    def test_last_value_single_partition(self):
        """Last value of entire dataset."""
        data = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}]
        result = last_value(data, field="name", order_by="id")

        for row in result:
            assert row["_last_name"] == "C"


class TestNtile:
    """Tests for ntile function."""

    def test_ntile_even_distribution(self):
        """Rows distributed evenly into buckets."""
        data = [{"id": i} for i in range(1, 5)]  # 4 rows
        result = ntile(data, n=2, order_by="id")

        # First 2 rows in bucket 1, last 2 in bucket 2
        assert result[0]["_ntile"] == 1
        assert result[1]["_ntile"] == 1
        assert result[2]["_ntile"] == 2
        assert result[3]["_ntile"] == 2

    def test_ntile_uneven_distribution(self):
        """Extra rows go to earlier buckets."""
        data = [{"id": i} for i in range(1, 6)]  # 5 rows
        result = ntile(data, n=3, order_by="id")

        # 5 rows into 3 buckets: 2, 2, 1
        buckets = [r["_ntile"] for r in result]
        assert buckets == [1, 1, 2, 2, 3]

    def test_ntile_invalid_n_raises(self):
        """n must be at least 1."""
        data = [{"id": 1}]
        with pytest.raises(ValueError):
            ntile(data, n=0)


class TestPercentRank:
    """Tests for percent_rank function."""

    def test_percent_rank_calculation(self):
        """Percent rank: (rank - 1) / (n - 1)."""
        data = [{"score": 10}, {"score": 20}, {"score": 30}]
        result = percent_rank(data, order_by="score")

        # Ranks: 1, 2, 3 for scores 10, 20, 30
        # Percent ranks: 0, 0.5, 1.0
        pr_10 = next(r for r in result if r["score"] == 10)
        pr_20 = next(r for r in result if r["score"] == 20)
        pr_30 = next(r for r in result if r["score"] == 30)

        assert pr_10["_percent_rank"] == 0.0
        assert pr_20["_percent_rank"] == 0.5
        assert pr_30["_percent_rank"] == 1.0

    def test_percent_rank_single_row(self):
        """Single row gets percent rank 0."""
        data = [{"score": 100}]
        result = percent_rank(data, order_by="score")

        assert result[0]["_percent_rank"] == 0.0


class TestCumeDist:
    """Tests for cume_dist function."""

    def test_cume_dist_calculation(self):
        """Cumulative distribution: (rows <= current) / total."""
        data = [{"score": 10}, {"score": 20}, {"score": 30}]
        result = cume_dist(data, order_by="score")

        cd_10 = next(r for r in result if r["score"] == 10)
        cd_20 = next(r for r in result if r["score"] == 20)
        cd_30 = next(r for r in result if r["score"] == 30)

        assert abs(cd_10["_cume_dist"] - 1/3) < 0.01
        assert abs(cd_20["_cume_dist"] - 2/3) < 0.01
        assert abs(cd_30["_cume_dist"] - 1.0) < 0.01

    def test_cume_dist_with_ties(self):
        """Ties get same cumulative distribution."""
        data = [
            {"name": "A", "score": 100},
            {"name": "B", "score": 100},
            {"name": "C", "score": 90},
        ]
        result = cume_dist(data, order_by="score")

        a = next(r for r in result if r["name"] == "A")
        b = next(r for r in result if r["name"] == "B")
        c = next(r for r in result if r["name"] == "C")

        assert c["_cume_dist"] == 1/3  # 90 is 1 out of 3
        assert a["_cume_dist"] == b["_cume_dist"] == 1.0  # Both 100s include all rows


class TestNestedFields:
    """Tests for window functions with nested field access."""

    def test_lag_nested_field(self):
        """Lag works with nested fields."""
        data = [
            {"id": 1, "user": {"name": "Alice"}},
            {"id": 2, "user": {"name": "Bob"}},
        ]
        result = lag(data, field="user.name", order_by="id")

        assert result[0]["_lag_user_name"] is None
        assert result[1]["_lag_user_name"] == "Alice"

    def test_partition_by_nested_field(self):
        """Partition by nested field."""
        data = [
            {"meta": {"dept": "A"}, "id": 1, "value": 10},
            {"meta": {"dept": "A"}, "id": 2, "value": 20},
            {"meta": {"dept": "B"}, "id": 1, "value": 30},
        ]
        result = row_number(data, partition_by="meta.dept", order_by="id")

        a1 = next(r for r in result if r["value"] == 10)
        a2 = next(r for r in result if r["value"] == 20)
        b1 = next(r for r in result if r["value"] == 30)

        assert a1["_row_number"] == 1
        assert a2["_row_number"] == 2
        assert b1["_row_number"] == 1


class TestMultipleOrderByKeys:
    """Tests for ordering by multiple fields."""

    def test_row_number_multiple_order_keys(self):
        """Order by multiple fields."""
        data = [
            {"dept": "A", "score": 90, "name": "Z"},
            {"dept": "A", "score": 90, "name": "A"},
            {"dept": "A", "score": 80, "name": "M"},
        ]
        result = row_number(data, order_by="score,name")

        z = next(r for r in result if r["name"] == "Z")
        a = next(r for r in result if r["name"] == "A")
        m = next(r for r in result if r["name"] == "M")

        # Order: 80/M, 90/A, 90/Z
        assert m["_row_number"] == 1
        assert a["_row_number"] == 2
        assert z["_row_number"] == 3
