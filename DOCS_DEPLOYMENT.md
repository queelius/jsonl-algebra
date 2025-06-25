# GitHub Pages Deployment

This project includes automated documentation deployment to GitHub Pages using MkDocs.

## Setup Instructions

### 1. Update Repository URLs
Edit `mkdocs.yml` and replace the placeholder URLs with your actual GitHub repository:

```yaml
site_url: https://yourusername.github.io/jsonl-algebra
repo_url: https://github.com/yourusername/jsonl-algebra
repo_name: yourusername/jsonl-algebra
```

### 2. Enable GitHub Pages (Manual Method)

**Option A: Using GitHub Actions (Recommended)**

1. Go to your repository on GitHub
2. Click on "Settings" tab
3. Scroll down to "Pages" in the left sidebar
4. Under "Source", select "GitHub Actions"
5. The workflow will automatically deploy docs when you push to main/master

**Option B: Manual Deployment from Local Machine**

1. Make sure you have push access to the repository
2. Run: `make docs-deploy`
3. This will build and push the docs to the `gh-pages` branch

### 3. Manual Deployment Commands

```bash
# Build documentation locally
make docs

# Serve documentation locally for development
make docs-serve

# Deploy to GitHub Pages (requires git remote)
make docs-deploy
```

## Automatic Deployment

The repository includes GitHub Actions workflows that will:

- **On every push/PR**: Run tests across Python 3.8-3.12
- **On push to main/master**: Automatically build and deploy documentation to GitHub Pages

## Accessing Documentation

Once deployed, your documentation will be available at:
`https://yourusername.github.io/jsonl-algebra`

## Troubleshooting

1. **Repository URLs**: Make sure the URLs in `mkdocs.yml` match your GitHub repository
2. **Branch name**: The workflow supports both `main` and `master` branches
3. **Permissions**: Ensure GitHub Actions has permission to deploy to Pages in repository settings
4. **Build errors**: Check the Actions tab for any build failures

## Local Development

To work on documentation locally:

```bash
# Install development dependencies
make install-dev

# Start local documentation server
make docs-serve

# Access at http://localhost:8000
```
