# Cache Service Browser v0.1.0

A browser UI for exploring cache entries stored in the MGraph-AI Cache Service.

## Features

- 🌙 **Dark Theme** - Trace Visualizers-inspired dark theme with cyan accents
- 📂 **Namespace Tabs** - Quick switching between cache namespaces
- 🌳 **File Tree Browser** - Hierarchical navigation of cache entries
- 👁️ **Multi-Format Viewer** - JSON, text, HTML, images with syntax highlighting
- ⛶ **Maximize Mode** - Full-screen content inspection
- ⌨️ **Keyboard Shortcuts** - Power user productivity (press `h` to see all)
- 🔐 **Auth Detection** - Graceful handling of authentication errors

## Project Structure

```
v0.1.0/
├── index.html                 # Main entry point
├── README.md                  # This file
├── css/
│   ├── common.css             # Dark theme variables, resets, shared styles
│   └── browser.css            # Main layout and component styles
├── js/
│   ├── browser.js             # Main orchestrator
│   ├── keyboard-shortcuts.js  # Keyboard shortcut manager
│   ├── services/
│   │   └── cache-api-client.js  # Cache Service API client
│   └── utils/
│       ├── helpers.js         # Utility functions
│       ├── base-component.js  # Web Component base class
│       └── component-paths.js # Path resolver for IFD versioning
├── data/
│   └── keyboard-shortcuts.json  # Keyboard shortcut definitions
└── components/
    ├── top-nav/               # Header with namespace tabs
    │   ├── top-nav.html
    │   ├── top-nav.css
    │   └── top-nav.js
    ├── file-tree/             # File browser sidebar
    │   ├── file-tree.html
    │   ├── file-tree.css
    │   └── file-tree.js
    └── content-viewer/        # Content display panel
        ├── content-viewer.html
        ├── content-viewer.css
        └── content-viewer.js
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `h` or `?` | Show/hide keyboard shortcuts help |
| `r` | Reload current content |
| `m` | Toggle maximize mode |
| `c` | Copy content to clipboard |
| `/` | Focus search/filter |
| `j` / `k` | Navigate next/previous file |
| `n` / `N` | Next/previous namespace |
| `1-5` | Jump to namespace 1-5 |
| `t` / `T` | Next/previous tab |
| `Escape` | Close dialogs or exit maximize |

## IFD (Iterative Flow Development) Notes

This is **v0.1.0** - a complete standalone implementation.

### Version Independence

- v0.1.0 is completely independent
- Future v0.1.x versions will reference v0.1.0 files via relative paths
- Each minor version only contains new/changed files
- v0.2.0 will be a fresh start with no v0.1.x dependencies

### Component Architecture

- **Web Components** (Custom Elements) with Shadow DOM
- **Event-driven** communication via CustomEvents
- **BaseComponent** base class handles resource loading
- **ComponentPaths** resolves version-relative paths

### File Organization

Each component is self-contained with:
- `component.html` - Template
- `component.css` - Styles  
- `component.js` - Logic

## API Endpoints Used

```
GET /info/health           # Health check
GET /server/storage/info   # Storage backend info
GET /namespaces/list       # Available namespaces
GET /admin/storage/files/all/{path}  # List all files
GET /admin/storage/file/json/{path}  # Get file content
```

## Browser Support

- Chrome 90+
- Firefox 90+
- Safari 15+
- Edge 90+

## Development

No build step required. Just serve the files with any HTTP server:

```bash
# Python
python -m http.server 8000

# Node.js
npx serve .

# Or access via the Cache Service's built-in static file serving
```

## Next Version (v0.1.1)

Planned additions:
- Search/filter improvements
- File metadata display
- refs/by-hash and refs/by-id relationship visualization
- Improved statistics panel
