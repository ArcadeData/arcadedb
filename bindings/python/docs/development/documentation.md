# Documentation Development

This guide explains how to work with the MkDocs Material documentation for ArcadeDB Python bindings.

## Documentation Structure

```
bindings/python/
├── docs/              # Documentation source
│   ├── index.md       # Homepage
│   ├── getting-started/
│   ├── guide/
│   ├── api/
│   ├── examples/
│   └── development/
├── mkdocs.yml         # MkDocs configuration
└── site/              # Built documentation (gitignored)
```

## Local Development

### Preview Documentation

Run a local development server with live reload:

```bash
cd bindings/python
mkdocs serve
```

Then open: **http://127.0.0.1:8000/arcadedb/**

Any changes to `.md` files will automatically refresh in your browser!

### Build Documentation

Build the static site to verify there are no errors:

```bash
mkdocs build --strict
```

The built site will be in `site/` directory.

### Check for Issues

```bash
# Check for broken links
mkdocs build --strict

# Validate configuration
mkdocs --version
```

## Versioned Documentation

Documentation is versioned using [mike](https://github.com/jimporter/mike) and automatically deployed when you create release tags.

### How It Works

1. **Create a GitHub Release** with tag like `python-X.Y.Z`
2. **GitHub Actions** automatically:
   - Builds documentation with MkDocs
   - Deploys version `X.Y.Z` to GitHub Pages
   - Sets it as the `latest` version
   - Updates version selector

3. **Users can view**:
   - Latest stable docs: <https://humemai.github.io/arcadedb/>
   - Specific version: <https://humemai.github.io/arcadedb/X.Y.Z/>
   - Version selector in top-right corner

### Deployment Workflow

**Automatic deployment** (recommended):

```bash
# 1. Make documentation changes on python-embedded branch
# 2. Build and test wheels
./build-all.sh headless
pytest

# 3. Commit and push changes
git add .
git commit -m "Release version X.Y.Z"
git push origin python-embedded

# 4. Create GitHub Release (creates tag automatically)
gh release create python-X.Y.Z \
  --title "Python Bindings vX.Y.Z" \
  --notes "Release notes"

# ✅ Docs automatically deploy to:
# https://humemai.github.io/arcadedb/X.Y.Z/ (versioned)
# https://humemai.github.io/arcadedb/ (redirects to latest)
```

**Manual deployment** (for testing):

You can manually trigger deployment from GitHub Actions:

1. Go to **Actions** → **Deploy MkDocs to GitHub Pages**
2. Click **Run workflow**
3. Choose:
   - **Version**: `dev` (or any version name)
   - **Set as latest**: `false` (to keep as separate version)

This creates a test deployment without affecting the stable docs.

### Version Management

List all deployed versions:

```bash
cd bindings/python
mike list
```

Delete a version (requires push access):

```bash
# Replace X.Y.Z with version to delete
mike delete X.Y.Z --push
```

Set a different version as default:

```bash
# Replace X.Y.Z with version to set as default
mike set-default X.Y.Z --push
```

### Version Alignment

Documentation versions **match PyPI package versions**:

| Release Tag | Docs Version | PyPI Packages |
|-------------|--------------|---------------|
| `python-X.Y.Z` | `X.Y.Z` | `arcadedb-embedded-*==X.Y.Z` |
| Example: `v25.9.1-python` | `25.9.1` | `arcadedb-embedded-*==25.9.1` |

This ensures users always see documentation matching their installed package version.

## Writing Documentation

### Style Guide

**Tone:**

- Friendly and approachable
- Use "you" to address the reader
- Keep sentences concise
- Use active voice

**Code Examples:**

- Show complete, runnable examples
- Include imports and setup
- Add comments for complex logic
- Use realistic variable names

**Organization:**

- Start with simple concepts
- Build to more complex topics
- Use clear headings
- Add navigation hints

### Markdown Features

#### Admonitions (Callouts)

```markdown
!!! note "Title (optional)"
    This is a note with a custom title.

!!! tip
    This is a helpful tip.

!!! warning
    This is a warning.

!!! danger
    This is a critical warning.

!!! info
    This is informational.

!!! success
    This indicates success.
```

#### Code Blocks with Tabs

```markdown
=== "Python"

    ```python
    import arcadedb_embedded as arcadedb
    ```

=== "SQL"

    ```sql
    SELECT * FROM User;
    ```
```

#### Code Block Highlighting

```python
# Line highlighting
```python hl_lines="2 3"
import arcadedb_embedded as arcadedb
db = arcadedb.create_database("./mydb")  # (1)!
db.close()
```

1. Creates a new database in the current directory
```
\```

#### Tables

```markdown
| Feature | Headless | Minimal | Full |
|---------|----------|---------|------|
| SQL | ✅ Yes | ✅ Yes | ✅ Yes |
| Gremlin | ❌ No | ❌ No | ✅ Yes |
```

#### Internal Links

```markdown
See [Installation Guide](../getting-started/installation.md) for details.

Link to a specific section: [Testing](testing.md#quick-start)
```

#### External Links

```markdown
Check the [official ArcadeDB docs](https://docs.arcadedb.com) for more.
```

### API Documentation

When documenting API methods, use this structure:

```markdown
## method_name()

Brief one-line description.

**Signature:**
```python
method_name(param1: type, param2: type = default) -> ReturnType
\```

**Parameters:**

- `param1` (type): Description of param1
- `param2` (type, optional): Description of param2. Defaults to `default`.

**Returns:**

- `ReturnType`: Description of return value

**Raises:**

- `ExceptionType`: When this exception occurs

**Example:**
```python
result = obj.method_name("value", param2=True)
\```

**See Also:**

- [Related Method](related.md)
\```

## Testing Documentation

### Verify All Links Work

```bash
# Build with strict mode (fails on warnings)
mkdocs build --strict
```

### Check Mobile Responsiveness

The Material theme is mobile-responsive by default. Test by:

1. Run `mkdocs serve`
2. Open in browser
3. Use browser DevTools responsive mode (F12 → Toggle device toolbar)
4. Test navigation, search, code blocks on mobile sizes

### Test Search

1. Run `mkdocs serve`
2. Click search icon (or press `/`)
3. Search for key terms
4. Verify results are relevant

## Continuous Integration

Documentation is automatically validated on every push via GitHub Actions:

- **Build check**: Ensures documentation builds without errors
- **Version deployment**: Deploys on tagged releases
- **Link validation**: Checks for broken links (TODO)

## Troubleshooting

### "Config file not found"

Make sure you're in `bindings/python/` directory:

```bash
cd bindings/python
mkdocs serve
```

### "Module not found" error

Install dependencies:

```bash
pip install mkdocs-material mkdocs-git-revision-date-localized-plugin
```

### Changes not appearing

1. Check file is saved
2. Check terminal for build errors
3. Hard refresh browser (Ctrl+Shift+R)
4. Restart `mkdocs serve`

### Version selector not showing

The version selector appears after deploying at least 2 versions with mike:

```bash
# Example: Deploy two versions
mike deploy X.Y.Z latest
mike deploy dev
```

## Next Steps

- [Contributing Guide](contributing.md) - How to contribute
- [Testing Guide](testing.md) - Running tests
- [MkDocs Material Reference](https://squidfunk.github.io/mkdocs-material/) - Full documentation
- [mike Documentation](https://github.com/jimporter/mike) - Versioning tool
