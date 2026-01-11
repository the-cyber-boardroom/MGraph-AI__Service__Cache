# Cache Service Browser v0.1.1

Adds Files View, Hashes View, draggable sidebar, and arrow key navigation.

## New Features

### Views (Tab bar in main content area)
- **Files View** (`1` key) - List cache entries by file_id
  - Search/filter files
  - Click to load full refs (config, metadata, refs)
  - Shows strategy badge (direct, temporal, key_based)
  - Detail panel with tabs: Content, Metadata, Config, Refs
  - Click through to Raw view for actual content

- **Hashes View** (`2` key) - List entries by content hash
  - Search/filter hashes
  - Shows file count per hash
  - Highlights duplicates (⚠️ multiple files with same hash)
  - Click to see all files sharing that hash
  - Click file to navigate to Files view

- **Raw View** (`3` key) - Physical file tree (v0.1.0 behavior)

### Draggable Sidebar
- Drag the edge to resize (200-600px range)
- Width is saved to localStorage

### Arrow Key Navigation
For the file tree (Raw view):
- `↑`/`↓` - Navigate between nodes
- `←` - Collapse folder or go to parent
- `→` - Expand folder or go to first child
- `Enter` - Select file or toggle folder

For Files/Hashes views:
- `↑`/`↓` or `j`/`k` - Navigate list
- `Enter` - Load details
- `/` - Focus search input

## IFD Structure

v0.1.1 extends v0.1.0 without modifying base files:

```
v0.1.1/
├── index.html              # Loads v0.1.0 + v0.1.1 resources
├── css/
│   └── patches.css         # Additional styles
├── js/
│   ├── patches.js          # Sidebar resize, tree keyboard nav
│   ├── api-extensions.js   # Additional API methods
│   └── browser-extensions.js # Orchestrator extensions
└── components/
    ├── view-tabs/          # Tab switcher component
    ├── files-view/         # Files list + detail
    └── hashes-view/        # Hashes list + detail
```

## API Endpoints Used (New)

```
GET /{namespace}/file-ids              # List all file IDs
GET /{namespace}/file-hashes           # List all content hashes
GET /{namespace}/retrieve/{id}/refs/all # Full refs for a file
GET /{namespace}/retrieve/hash/{hash}/refs-hash # Hash ref details
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `1` | Switch to Files view |
| `2` | Switch to Hashes view |
| `3` | Switch to Raw view |
| `↑`/`↓` | Navigate items |
| `←`/`→` | Collapse/expand (tree) |
| `Enter` | Select item |
| `/` | Focus search |
| `h` or `?` | Show all shortcuts |

## Changes from v0.1.0

- Added view-tabs component in main content area
- Added files-view component (by file_id)
- Added hashes-view component (by hash)
- Sidebar now draggable
- Arrow keys work in file tree
- Number keys (1-3) switch views
- Sidebar hidden in Files/Hashes views
