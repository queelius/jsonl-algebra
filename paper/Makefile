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

# --- HTML via tex2html ---
HTMLDIR = html_paper

.PHONY: html

html: $(HTMLDIR)/index.html

$(HTMLDIR)/index.html: main.tex
	@mkdir -p $(HTMLDIR)
	tex2html main.tex -o $(HTMLDIR)
