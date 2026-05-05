# MOVE-OD Documentation

This directory contains the documentation for MOVE-OD, including the GitHub Pages site.

## Structure

- `index.md` - Homepage for GitHub Pages
- `USER_GUIDE.md` - Complete user guide
- `_config.yml` - Jekyll configuration for GitHub Pages
- `assets/css/style.css` - Custom styling

## Building Locally

To test the GitHub Pages site locally:

```bash
# Install Jekyll
gem install bundler jekyll

# Create Gemfile
cat > Gemfile << EOF
source 'https://rubygems.org'
gem 'github-pages', group: :jekyll_plugins
EOF

# Install dependencies
bundle install

# Serve locally
bundle exec jekyll serve

# Open browser to http://localhost:4000
```

## Deploying to GitHub Pages

1. Push this directory to your repository
2. Go to Settings > Pages
3. Select source: Deploy from a branch
4. Select branch: main (or master)
5. Select folder: `/docs`
6. Click Save

Your site will be available at: `https://yourusername.github.io/move_od/`

## Updating Documentation

### Adding New Pages

1. Create a new `.md` file in this directory
2. Add front matter:
   ```yaml
   ---
   layout: default
   title: Your Page Title
   ---
   ```
3. Write your content in Markdown
4. Link to it from `index.md` or other pages

### Modifying Styles

Edit `assets/css/style.css` to customize the appearance.

### Images

Place images in `assets/images/` and reference them:

```markdown
![Alt text](assets/images/your-image.png)
```

## Links

- [Live Site](https://yourusername.github.io/move_od/)
- [Main Repository](https://github.com/yourusername/move_od)
- [GitHub Pages Documentation](https://docs.github.com/en/pages)
