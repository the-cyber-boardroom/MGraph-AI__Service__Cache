# Cache Service Browser v0.1.2

Adds HTML viewer, fixes keyboard navigation, scroll isolation, JSON truncation, and adds Open in Browser link.

## New Features

### HTML Viewer
When viewing JSON files that contain an `html` field (like `{"html": "<!DOCTYPE html>..."}`), a new **HTML tab** appears:

- **🌐 Preview** - Renders the HTML in a sandboxed iframe
- **📄 Source** - Shows the raw HTML source code
- **📋 Copy** - Copies the HTML to clipboard
- **⛶ Maximize** - Opens full-screen HTML preview modal

The HTML tab shows a `field` badge when HTML was extracted from a JSON field (vs raw HTML file).

### 🌐 Open in Browser
New button in the content viewer header that opens the file directly via the cache service API:
- URL format: `http://localhost:10017/admin/storage/file/json/{encoded_path}`
- Opens in a new tab
- Useful for accessing raw API responses

### Keyboard Navigation Fix
The file tree now properly handles keyboard navigation:

1. **Click anywhere in the tree** to focus it
2. Once focused, use arrow keys:
   - `↑`/`↓` - Move between files/folders
   - `←` - Collapse folder or move to parent
   - `→` - Expand folder or move to first child
   - `Enter`/`Space` - Select file or toggle folder

The tree shows a subtle focus indicator when keyboard navigation is active.

## Bug Fixes

### Scroll Isolation (Fixed)
The sidebar (file tree) and main content area now scroll independently. Previously, scrolling the tree would also scroll the content viewer off-screen.

### JSON String Truncation (Fixed)
JSON strings are now displayed in full instead of being truncated at 500 characters. Long HTML content and other data is fully visible.

## Files Changed

```
v0.1.2/
├── index.html              # Entry point (loads v0.1.1 + v0.1.2)
├── css/
│   ├── common.css          # Import chain to v0.1.0
│   └── patches.css         # Scroll fix, HTML viewer, JSON display
├── js/
│   ├── patches.js          # JsonTruncationFix, TreeKeyboardNavFix, HtmlViewerModal
│   └── browser-extensions.js # HTML tab injection, Open in Browser button
└── components/
    └── html-viewer/        # Standalone HTML viewer component
```

## API Integration

The "Open in Browser" button constructs URLs using:
```
http://localhost:10017/admin/storage/file/json/{encoded_path}
```

This allows direct access to the underlying cache service API.
