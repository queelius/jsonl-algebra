# Deployment Quick Start

## TL;DR

### Deploy to GitHub Pages (Automatic)
```bash
# Just push your changes
git add docs/ mkdocs.yml
git commit -m "Update docs"
git push origin master

# GitHub Actions will automatically deploy to:
# https://queelius.github.io/jsonl-algebra/
```

### Deploy to Hugo Blog
```bash
# 1. Edit the script with your blog path (first time only)
vim deploy-to-hugo.sh
# Set: HUGO_BLOG_DIR="$HOME/your-blog-path"

# 2. Run the deployment script
./deploy-to-hugo.sh

# 3. The script will tell you what to do next
```

## Detailed Steps

### First Time Setup

#### For GitHub Pages:
1. **Enable GitHub Pages in your repo:**
   - Go to Settings → Pages
   - Set Source to: **GitHub Actions**

2. **Push to deploy:**
   ```bash
   git push origin master
   ```

3. **Visit your docs:**
   ```
   https://queelius.github.io/jsonl-algebra/
   ```

#### For Hugo Blog:

1. **Configure the deployment script:**
   ```bash
   # Edit deploy-to-hugo.sh
   vim deploy-to-hugo.sh

   # Change this line to your blog's path:
   HUGO_BLOG_DIR="$HOME/blog"  # ← Edit this
   ```

2. **Run the script:**
   ```bash
   chmod +x deploy-to-hugo.sh
   ./deploy-to-hugo.sh
   ```

3. **Follow the script's instructions to push to your blog**

### Regular Updates

#### Update Docs:
```bash
# 1. Edit your documentation
vim docs/guide/repl/introduction.md

# 2. Build locally to test
mkdocs serve
# Visit http://127.0.0.1:8000

# 3. Deploy
git add docs/
git commit -m "Update REPL documentation"
git push  # Triggers GitHub Pages deploy

# 4. (Optional) Deploy to Hugo blog
./deploy-to-hugo.sh
cd ~/blog
git add static/projects/jsonl-algebra/
git commit -m "Update JSONL Algebra docs"
git push
```

## Where Will Your Docs Be?

### GitHub Pages:
- URL: `https://queelius.github.io/jsonl-algebra/`
- Updates: Automatic on push to master
- Content: Entire MkDocs site

### Hugo Blog:
- URL: `https://yourblog.com/projects/jsonl-algebra/`
- Updates: Manual (run `./deploy-to-hugo.sh`)
- Content: Copy of MkDocs site in Hugo's static files

## Testing

### Test MkDocs locally:
```bash
mkdocs serve
# Visit http://127.0.0.1:8000
```

### Test Hugo integration locally:
```bash
./deploy-to-hugo.sh
cd ~/blog
hugo server
# Visit http://localhost:1313/projects/jsonl-algebra/
```

## Troubleshooting

### GitHub Pages not deploying?
1. Check Settings → Pages is set to "GitHub Actions"
2. Check Actions tab for workflow errors
3. Ensure mkdocs.yml is valid: `mkdocs build`

### Hugo script fails?
1. Check HUGO_BLOG_DIR path is correct
2. Ensure MkDocs is installed: `pip install mkdocs-material`
3. Check you have write permissions to Hugo blog directory

### Docs look broken?
1. Check all images use relative paths
2. Rebuild: `mkdocs build --clean`
3. Check browser console for errors

## Advanced: Automated Hugo Deployment

If your Hugo blog is on GitHub, see `DEPLOYMENT.md` for GitHub Actions automation.

## Questions?

See the full guide: [DEPLOYMENT.md](DEPLOYMENT.md)
