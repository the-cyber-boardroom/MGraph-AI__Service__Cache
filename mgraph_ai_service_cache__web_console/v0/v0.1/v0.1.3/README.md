# Cache Service Browser v0.1.3

3-column layout with dedicated HTML preview panel.

## Major Changes

### 3-Column Layout
```
┌─────────────┬─────────────────────┬─────────────────┐
│   Sidebar   │    Content Viewer   │   HTML Preview  │
│  (File Tree)│   (Formatted/Raw)   │    (3rd col)    │
│             │                     │                 │
│ ◀──resize──▶│                     │◀────resize────▶ │
└─────────────┴─────────────────────┴─────────────────┘
```

- **Column 1**: File tree (200-600px, resizable)
- **Column 2**: Content viewer with tabs
- **Column 3**: HTML preview panel (300-800px, resizable, auto-shows when HTML detected)

### Dual Resize Handles
Both resize handles work like the original sidebar resizer:
- Drag to resize
- Visual feedback on hover/drag
- Width persisted to localStorage

### Proper Scroll Isolation (FIXED)
Each column now scrolls independently:
- Sidebar scrolls without moving content
- Content viewer scrolls without moving sidebar
- HTML panel has its own scroll

### HTML Preview Panel
Replaces the inline HTML tab from v0.1.2:
- **Auto-shows** when JSON with `html` field is loaded
- **Auto-hides** when non-HTML content is loaded
- Preview/Source toggle
- Copy and Open in New Tab buttons
- Close button to manually hide

## Files Changed

```
v0.1.3/
├── index.html              # 3-column layout structure
├── css/
│   ├── common.css          # Import chain
│   └── patches.css         # 3-column layout, resize handles, panel styles
├── js/
│   ├── patches.js          # ColumnResizer, HtmlPanelManager, ScrollEnforcer
│   └── browser-extensions.js # HTML detection → panel integration
└── components/
    └── html-panel/         # New 3rd column component
        ├── html-panel.html
        ├── html-panel.css
        └── html-panel.js
```

## How It Works

1. **File selection**: User clicks a file in the tree
2. **Content loads**: content-viewer displays JSON
3. **HTML detection**: v0.1.3 extension checks for `html` field
4. **Panel shows**: If HTML found, 3rd column appears with preview
5. **Panel hides**: When different file selected without HTML

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `↑`/`↓` | Navigate tree (when focused) |
| `←`/`→` | Collapse/expand folders |
| `Enter` | Select file |

## CSS Architecture

The scroll isolation fix requires specific CSS on each container:

```css
.app-main {
    display: flex;
    overflow: hidden !important;
    min-height: 0;
}

.sidebar, .main-content, .html-preview-panel {
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
}

/* Internal scrolling */
.tree-content, .viewer-body, .html-panel-body {
    flex: 1;
    min-height: 0;
    overflow: auto;
}
```

The key is `min-height: 0` on flex children to allow them to shrink and scroll internally.
