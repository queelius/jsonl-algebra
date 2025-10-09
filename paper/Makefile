.PHONY: all clean pdf view

# Main target
all: pdf

# Compile PDF
pdf: main.pdf

main.pdf: main.tex
	pdflatex main.tex
	pdflatex main.tex  # Run twice for references

# View PDF
view: main.pdf
	xdg-open main.pdf || open main.pdf || start main.pdf

# Clean build artifacts
clean:
	rm -f *.aux *.log *.out *.toc *.bbl *.blg *.pdf

# Clean everything including PDF
distclean: clean
	rm -f main.pdf
