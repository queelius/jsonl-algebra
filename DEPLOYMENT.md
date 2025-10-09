# Documentation Deployment Guide

This guide covers deploying the MkDocs documentation to GitHub Pages and integrating it with your Hugo blog.

## Option 1: GitHub Pages (Automated)

### Setup

The repository is already configured with GitHub Actions for automatic deployment.

#### 1. Enable GitHub Pages

1. Go to your repository settings on GitHub
2. Navigate to **Settings > Pages**
3. Under "Build and deployment":
   - Source: **GitHub Actions**
   - (Not "Deploy from a branch")

#### 2. Push to Trigger Deployment

```bash
# Make sure your docs are committed
git add docs/ mkdocs.yml
git commit -m "Update documentation"
git push origin master
```

The workflow will automatically:
- Build the MkDocs site
- Deploy to GitHub Pages
- Make it available at `https://queelius.github.io/jsonl-algebra/`

#### 3. Monitor Deployment

- Go to **Actions** tab in your GitHub repo
- Watch the "Deploy Documentation" workflow
- Once complete, visit your GitHub Pages URL

### Manual Deployment (Alternative)

If you prefer manual control:

```bash
# Install MkDocs
pip install mkdocs-material mkdocstrings[python]

# Build and deploy in one command
mkdocs gh-deploy

# This will:
# 1. Build the site to ./site/
# 2. Push to gh-pages branch
# 3. Make it live on GitHub Pages
```

## Option 2: Hugo Blog Integration

You have two main approaches to integrate MkDocs output into your Hugo blog:

### Approach A: Build and Copy to Hugo Static Directory

This is the simplest approach - build MkDocs to your Hugo static files.

#### 1. Build MkDocs Site

```bash
# In jsonl-algebra directory
mkdocs build

# Output will be in ./site/
```

#### 2. Copy to Hugo Blog

```bash
# Copy the built site to your Hugo blog's static directory
# Assuming your Hugo blog is at ~/blog/

# Create a subdirectory for the docs
mkdir -p ~/blog/static/projects/jsonl-algebra

# Copy the MkDocs output
cp -r site/* ~/blog/static/projects/jsonl-algebra/

# Now docs will be available at:
# https://yourblog.com/projects/jsonl-algebra/
```

#### 3. Automate with Script

Create `deploy-to-hugo.sh`:

```bash
#!/bin/bash
set -e

HUGO_BLOG_DIR="$HOME/blog"  # Adjust to your Hugo blog path
DOCS_SUBPATH="projects/jsonl-algebra"

echo "Building MkDocs site..."
mkdocs build

echo "Copying to Hugo blog..."
mkdir -p "$HUGO_BLOG_DIR/static/$DOCS_SUBPATH"
cp -r site/* "$HUGO_BLOG_DIR/static/$DOCS_SUBPATH/"

echo "Done! Docs copied to $HUGO_BLOG_DIR/static/$DOCS_SUBPATH"
echo "Don't forget to commit and deploy your Hugo blog!"
```

Make it executable:
```bash
chmod +x deploy-to-hugo.sh
./deploy-to-hugo.sh
```

#### 4. Add to Hugo Blog's Git

```bash
cd ~/blog
git add static/projects/jsonl-algebra/
git commit -m "Update JSONL Algebra docs"
git push  # Or however you deploy your Hugo blog
```

### Approach B: Hugo Mounted Module (Advanced)

Use Hugo's module system to mount the built docs.

#### 1. Configure Hugo

In your Hugo blog's `config.toml` or `hugo.toml`:

```toml
[module]
[[module.mounts]]
  source = "/path/to/jsonl-algebra/site"
  target = "static/projects/jsonl-algebra"
```

Or in `config.yaml`:

```yaml
module:
  mounts:
    - source: /path/to/jsonl-algebra/site
      target: static/projects/jsonl-algebra
```

#### 2. Build MkDocs First

```bash
cd /path/to/jsonl-algebra
mkdocs build
```

#### 3. Build Hugo

```bash
cd ~/blog
hugo  # Will include mounted docs
```

### Approach C: Subdomain or Reverse Proxy

Host the docs separately and link from your blog:

#### Option 1: GitHub Pages Subdomain

1. Deploy to GitHub Pages (see Option 1 above)
2. Add a CNAME in your repo for custom subdomain:
   ```bash
   echo "docs-jsonl-algebra.yourdomain.com" > docs/CNAME
   git add docs/CNAME
   git commit -m "Add custom domain"
   git push
   ```

3. Configure DNS:
   - Add CNAME record: `docs-jsonl-algebra` → `queelius.github.io`

4. Link from Hugo blog:
   ```markdown
   [JSONL Algebra Documentation](https://docs-jsonl-algebra.yourdomain.com)
   ```

#### Option 2: Netlify/Vercel Hosting

1. Build MkDocs:
   ```bash
   mkdocs build
   ```

2. Deploy `./site/` to Netlify or Vercel

3. Link from Hugo blog

## Recommended Workflow

For your use case, I recommend **Approach A** (Build and Copy):

### Complete Setup

1. **Create deployment script** in jsonl-algebra repo:

```bash
cat > deploy-to-hugo.sh << 'EOF'
#!/bin/bash
set -e

# Configuration
HUGO_BLOG_DIR="$HOME/blog"  # Adjust this
DOCS_PATH="static/projects/jsonl-algebra"

# Build MkDocs
echo "📚 Building MkDocs documentation..."
mkdocs build --clean

# Prepare Hugo directory
echo "📂 Preparing Hugo blog directory..."
mkdir -p "$HUGO_BLOG_DIR/$DOCS_PATH"

# Copy files
echo "📋 Copying documentation..."
rsync -av --delete site/ "$HUGO_BLOG_DIR/$DOCS_PATH/"

# Report
echo "✅ Done!"
echo ""
echo "Documentation deployed to: $HUGO_BLOG_DIR/$DOCS_PATH"
echo "Will be available at: https://yourblog.com/projects/jsonl-algebra/"
echo ""
echo "Next steps:"
echo "  cd $HUGO_BLOG_DIR"
echo "  git add $DOCS_PATH"
echo "  git commit -m 'Update JSONL Algebra docs'"
echo "  hugo server  # Test locally"
echo "  git push     # Deploy"
EOF

chmod +x deploy-to-hugo.sh
```

2. **Add to your workflow**:

```bash
# When you update docs
vim docs/guide/repl/introduction.md

# Deploy to Hugo blog
./deploy-to-hugo.sh

# Then go to your Hugo blog and push
cd ~/blog
git add static/projects/jsonl-algebra/
git commit -m "Update JSONL Algebra documentation"
git push
```

3. **Create a Hugo blog post** to introduce the docs:

```markdown
---
title: "JSONL Algebra Documentation"
date: 2024-10-08
tags: ["tools", "data", "jsonl"]
---

I've created comprehensive documentation for JSONL Algebra, a powerful
command-line tool for manipulating JSONL data.

[**View the full documentation →**](/projects/jsonl-algebra/)

## Quick Links

- [Getting Started](/projects/jsonl-algebra/getting-started/quickstart/)
- [Interactive REPL Guide](/projects/jsonl-algebra/guide/repl/introduction/)
- [API Reference](/projects/jsonl-algebra/reference/)

## What is JSONL Algebra?

[Brief description...]
```

## Automatic Updates (Optional)

### GitHub Actions to Auto-Deploy to Hugo

If your Hugo blog is also on GitHub, you can automate the entire process:

```yaml
# .github/workflows/deploy-to-hugo.yml
name: Deploy Docs to Hugo Blog

on:
  push:
    branches: [master]
    paths:
      - 'docs/**'
      - 'mkdocs.yml'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout jsonl-algebra
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: 3.x

      - name: Install MkDocs
        run: pip install mkdocs-material mkdocstrings[python]

      - name: Build MkDocs
        run: mkdocs build

      - name: Checkout Hugo blog
        uses: actions/checkout@v4
        with:
          repository: yourusername/your-hugo-blog
          token: ${{ secrets.HUGO_BLOG_TOKEN }}
          path: hugo-blog

      - name: Copy docs to Hugo
        run: |
          mkdir -p hugo-blog/static/projects/jsonl-algebra
          cp -r site/* hugo-blog/static/projects/jsonl-algebra/

      - name: Commit and push to Hugo blog
        run: |
          cd hugo-blog
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add static/projects/jsonl-algebra/
          git commit -m "Update JSONL Algebra docs from ${{ github.sha }}" || exit 0
          git push
```

You'd need to:
1. Create a Personal Access Token (PAT) with repo permissions
2. Add it as `HUGO_BLOG_TOKEN` secret in your jsonl-algebra repo
3. Update `yourusername/your-hugo-blog` to your actual Hugo repo

## Testing Locally

### Test MkDocs Site

```bash
mkdocs serve
# Visit http://127.0.0.1:8000
```

### Test in Hugo Blog

```bash
./deploy-to-hugo.sh
cd ~/blog
hugo server
# Visit http://localhost:1313/projects/jsonl-algebra/
```

## Summary

**Easiest approach for Hugo blog integration:**

1. ✅ Build with `mkdocs build`
2. ✅ Copy `site/` to `~/blog/static/projects/jsonl-algebra/`
3. ✅ Commit and push your Hugo blog
4. ✅ Docs appear at `yourblog.com/projects/jsonl-algebra/`

**Automatic GitHub Pages (also works):**
- Already configured via `.github/workflows/docs.yml`
- Just push to master
- Live at `queelius.github.io/jsonl-algebra`

**Best of both worlds:**
- Use GitHub Pages for the official docs
- Copy to Hugo blog for integrated experience
- Keep both in sync with the deploy script
