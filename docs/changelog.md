# Changelog

All notable changes to **jsonl-algebra** are documented here.

---

## v1.04 — 2026-01-31

**Documentation & release maintenance.**

- Trimmed MkDocs nav to reference only pages that exist on disk
- Added this changelog
- Fixed stale `bump2version` config (was pinned to v0.9.0)

---

## v1.03 — 2025-12-14

**Window functions, join types, and shell enhancements.**

- Added window functions: `row_number`, `rank`, `dense_rank`, `lag`, `lead`,
  `first_value`, `last_value`, `ntile`, `percent_rank`, `cume_dist`
- Extended join types (left, right, outer)
- Shell UX improvements and bug fixes

---

## v1.02 — 2025-11-14

**MCP server fixes and developer documentation.**

- Fixed critical MCP server bugs
- Added developer documentation and integration guides
- Fixed build configuration (removed non-existent `scripts` package)
- PyPI release

---

## v1.01 — 2025-08-18

**Fluid API and initial PyPI release.**

- Added fluid API for chaining relational operations
- Chained group-by and aggregation support
- Initial MkDocs documentation site with Material theme
- First PyPI release

---

## v1.0 — 2025-10-27

**Interactive shell and comprehensive test suite.**

- Added `ja-shell`: interactive filesystem navigator for JSON/JSONL
- Virtual filesystem abstraction (`ja/vfs.py`) — treat JSONL files as
  directories of records
- MCP (Model Context Protocol) server integration
- Comprehensive test suite with TDD approach
- MkDocs documentation deployed to GitHub Pages

---

## v0.9.0 — 2025-08-18

**Initial release.**

- Core relational algebra operations: `select`, `project`, `join`, `union`,
  `difference`, `intersection`, `distinct`, `sort_by`, `rename`, `product`
- Expression evaluation engine with dot notation and JMESPath fallback
- Group-by with aggregation (`groupby_agg`)
- Pipeline composition with `|` operator
- CLI tool (`ja`) and streaming-compatible design
- CSV import/export, JSON Schema inference
