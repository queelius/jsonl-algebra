# JSONL Algebra Technical Paper

This directory contains the LaTeX source for the technical paper on JSONL Algebra.

## Building the Paper

### Requirements

- LaTeX distribution (TeX Live, MiKTeX, or MacTeX)
- pdflatex command

### Build Commands

```bash
# Build PDF
make pdf

# Build and view
make view

# Clean build artifacts
make clean

# Clean everything including PDF
make distclean
```

### Manual Build

If you don't have `make`:

```bash
pdflatex main.tex
pdflatex main.tex  # Run twice for proper references
```

## Output

The compiled paper will be `main.pdf`.

## Contents

- `main.tex` - Main LaTeX source
- `Makefile` - Build automation
- `README.md` - This file

## Abstract

The paper presents JSONL Algebra (ja), a command-line tool and interactive REPL that applies relational algebra operations to semi-structured JSONL data. It introduces a novel interactive workspace model enabling exploratory data analysis through named datasets with immediate execution.

## Sections

1. Introduction
2. Background and Related Work
3. System Design
4. Implementation
5. Evaluation
6. Discussion
7. Conclusion
