#!/bin/bash
# Deploy MkDocs documentation to Hugo blog
set -e

# ============================================================================
# Configuration - EDIT THESE VALUES
# ============================================================================

# Path to your Hugo blog repository
HUGO_BLOG_DIR="$HOME/blog"

# Subdirectory within Hugo's static/ folder where docs should go
# Docs will be available at: https://yourblog.com/projects/jsonl-algebra/
DOCS_SUBPATH="projects/jsonl-algebra"

# ============================================================================
# Script (no need to edit below this line)
# ============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Hugo blog directory exists
if [ ! -d "$HUGO_BLOG_DIR" ]; then
    echo -e "${RED}❌ Error: Hugo blog directory not found at: $HUGO_BLOG_DIR${NC}"
    echo -e "${YELLOW}Please edit this script and set the correct HUGO_BLOG_DIR${NC}"
    exit 1
fi

# Check if MkDocs is installed
if ! command -v mkdocs &> /dev/null; then
    echo -e "${RED}❌ Error: mkdocs not found${NC}"
    echo -e "${YELLOW}Install with: pip install mkdocs-material mkdocstrings[python]${NC}"
    exit 1
fi

# Build MkDocs documentation
echo -e "${BLUE}📚 Building MkDocs documentation...${NC}"
mkdocs build --clean

if [ ! -d "site" ]; then
    echo -e "${RED}❌ Error: MkDocs build failed - site/ directory not found${NC}"
    exit 1
fi

# Prepare Hugo directory
HUGO_DOCS_DIR="$HUGO_BLOG_DIR/static/$DOCS_SUBPATH"
echo -e "${BLUE}📂 Preparing Hugo blog directory...${NC}"
mkdir -p "$HUGO_DOCS_DIR"

# Copy files
echo -e "${BLUE}📋 Copying documentation files...${NC}"
if command -v rsync &> /dev/null; then
    # Use rsync if available (better for updates)
    rsync -av --delete site/ "$HUGO_DOCS_DIR/"
else
    # Fallback to cp
    rm -rf "$HUGO_DOCS_DIR"/*
    cp -r site/* "$HUGO_DOCS_DIR/"
fi

# Count files copied
FILE_COUNT=$(find "$HUGO_DOCS_DIR" -type f | wc -l)

# Report success
echo ""
echo -e "${GREEN}✅ Done! Documentation successfully deployed.${NC}"
echo ""
echo -e "${BLUE}📊 Statistics:${NC}"
echo -e "   Files copied: $FILE_COUNT"
echo -e "   Destination:  $HUGO_DOCS_DIR"
echo ""
echo -e "${BLUE}🌐 URLs:${NC}"
echo -e "   Local:  http://localhost:1313/$DOCS_SUBPATH/"
echo -e "   Live:   https://yourblog.com/$DOCS_SUBPATH/"
echo ""
echo -e "${YELLOW}📝 Next steps:${NC}"
echo -e "   1. Test locally:"
echo -e "      ${BLUE}cd $HUGO_BLOG_DIR${NC}"
echo -e "      ${BLUE}hugo server${NC}"
echo -e "      ${BLUE}# Visit http://localhost:1313/$DOCS_SUBPATH/${NC}"
echo ""
echo -e "   2. Deploy to production:"
echo -e "      ${BLUE}cd $HUGO_BLOG_DIR${NC}"
echo -e "      ${BLUE}git add static/$DOCS_SUBPATH${NC}"
echo -e "      ${BLUE}git commit -m 'Update JSONL Algebra docs'${NC}"
echo -e "      ${BLUE}git push${NC}"
echo ""
