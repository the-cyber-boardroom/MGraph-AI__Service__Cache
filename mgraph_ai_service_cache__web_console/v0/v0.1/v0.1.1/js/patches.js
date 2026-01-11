/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.1 Patches
   
   Extensions to v0.1.0 base:
   - Draggable sidebar resize
   - Arrow key navigation for file tree
   - Extended keyboard shortcuts
   ═══════════════════════════════════════════════════════════════════════════════ */

// ═══════════════════════════════════════════════════════════════════════════════
// Sidebar Resize
// ═══════════════════════════════════════════════════════════════════════════════

class SidebarResizer {
    constructor() {
        this.sidebar = null;
        this.handle = null;
        this.isResizing = false;
        this.startX = 0;
        this.startWidth = 0;
        this.minWidth = 200;
        this.maxWidth = 600;
    }

    init() {
        this.sidebar = document.getElementById('sidebar');
        this.handle = document.getElementById('sidebar-resize-handle');
        
        if (!this.sidebar || !this.handle) {
            console.warn('Sidebar or resize handle not found');
            return;
        }

        // Position handle at right edge of sidebar
        this.updateHandlePosition();

        // Bind events
        this.handle.addEventListener('mousedown', (e) => this.startResize(e));
        document.addEventListener('mousemove', (e) => this.resize(e));
        document.addEventListener('mouseup', () => this.stopResize());

        // Load saved width
        const savedWidth = localStorage.getItem('cache-browser-sidebar-width');
        if (savedWidth) {
            const width = parseInt(savedWidth, 10);
            if (width >= this.minWidth && width <= this.maxWidth) {
                this.sidebar.style.width = `${width}px`;
                this.updateHandlePosition();
            }
        }

        console.log('✅ Sidebar resizer initialized');
    }

    startResize(e) {
        this.isResizing = true;
        this.startX = e.clientX;
        this.startWidth = this.sidebar.offsetWidth;
        
        this.sidebar.classList.add('resizing');
        this.handle.classList.add('dragging');
        document.body.classList.add('sidebar-resizing');
        
        e.preventDefault();
    }

    resize(e) {
        if (!this.isResizing) return;

        const diff = e.clientX - this.startX;
        let newWidth = this.startWidth + diff;

        // Clamp to min/max
        newWidth = Math.max(this.minWidth, Math.min(this.maxWidth, newWidth));

        this.sidebar.style.width = `${newWidth}px`;
        this.updateHandlePosition();
    }

    stopResize() {
        if (!this.isResizing) return;

        this.isResizing = false;
        this.sidebar.classList.remove('resizing');
        this.handle.classList.remove('dragging');
        document.body.classList.remove('sidebar-resizing');

        // Save width
        localStorage.setItem('cache-browser-sidebar-width', this.sidebar.offsetWidth);
    }

    updateHandlePosition() {
        if (!this.sidebar || !this.handle) return;
        const sidebarRect = this.sidebar.getBoundingClientRect();
        this.handle.style.left = `${sidebarRect.right - 3}px`;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// File Tree Arrow Key Navigation Extension
// ═══════════════════════════════════════════════════════════════════════════════

class TreeKeyboardNav {
    constructor() {
        this.fileTree = null;
        this.focusedNode = null;
    }

    init() {
        // Wait for file-tree to be ready
        const checkTree = setInterval(() => {
            this.fileTree = document.querySelector('file-tree');
            if (this.fileTree) {
                clearInterval(checkTree);
                this.setupListeners();
                console.log('✅ Tree keyboard navigation initialized');
            }
        }, 100);

        // Give up after 10 seconds
        setTimeout(() => clearInterval(checkTree), 10000);
    }

    setupListeners() {
        document.addEventListener('keydown', (e) => this.handleKeydown(e));
    }

    handleKeydown(e) {
        // Only handle in raw view
        const activePanel = document.querySelector('.view-panel.active');
        if (!activePanel || activePanel.id !== 'raw-view-panel') return;

        // Don't handle if in input
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

        // Get the file tree's shadow root
        const shadowRoot = this.fileTree?.shadowRoot;
        if (!shadowRoot) return;

        const treeContent = shadowRoot.querySelector('#tree-content');
        if (!treeContent) return;

        const nodes = Array.from(treeContent.querySelectorAll('.tree-node'));
        if (nodes.length === 0) return;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                this.navigateVertical(nodes, 1);
                break;
            case 'ArrowUp':
                e.preventDefault();
                this.navigateVertical(nodes, -1);
                break;
            case 'ArrowRight':
                e.preventDefault();
                this.expandOrDescend(nodes);
                break;
            case 'ArrowLeft':
                e.preventDefault();
                this.collapseOrAscend(nodes);
                break;
            case 'Enter':
                e.preventDefault();
                this.selectFocused(nodes);
                break;
        }
    }

    navigateVertical(nodes, direction) {
        // Find visible nodes only
        const visibleNodes = nodes.filter(node => {
            const rect = node.getBoundingClientRect();
            return rect.height > 0;
        });

        if (visibleNodes.length === 0) return;

        // Find currently focused node
        let currentIndex = visibleNodes.findIndex(node => 
            node.classList.contains('keyboard-focused')
        );

        // Remove current focus
        visibleNodes.forEach(node => node.classList.remove('keyboard-focused'));

        // Calculate new index
        if (currentIndex === -1) {
            currentIndex = direction > 0 ? 0 : visibleNodes.length - 1;
        } else {
            currentIndex += direction;
            if (currentIndex < 0) currentIndex = 0;
            if (currentIndex >= visibleNodes.length) currentIndex = visibleNodes.length - 1;
        }

        // Focus new node
        const newFocused = visibleNodes[currentIndex];
        newFocused.classList.add('keyboard-focused');
        this.focusedNode = newFocused;

        // Scroll into view
        const content = newFocused.querySelector('.tree-node-content');
        if (content) {
            content.scrollIntoView({ block: 'nearest' });
        }
    }

    expandOrDescend(nodes) {
        if (!this.focusedNode) return;

        const isFolder = this.focusedNode.classList.contains('folder');
        const isExpanded = this.focusedNode.classList.contains('expanded');

        if (isFolder && !isExpanded) {
            // Expand folder
            const toggle = this.focusedNode.querySelector('.tree-node-toggle');
            if (toggle) toggle.click();
        } else if (isFolder && isExpanded) {
            // Descend to first child
            const children = this.focusedNode.querySelector('.tree-node-children');
            if (children) {
                const firstChild = children.querySelector('.tree-node');
                if (firstChild) {
                    this.focusedNode.classList.remove('keyboard-focused');
                    firstChild.classList.add('keyboard-focused');
                    this.focusedNode = firstChild;
                    firstChild.querySelector('.tree-node-content')?.scrollIntoView({ block: 'nearest' });
                }
            }
        }
    }

    collapseOrAscend(nodes) {
        if (!this.focusedNode) return;

        const isFolder = this.focusedNode.classList.contains('folder');
        const isExpanded = this.focusedNode.classList.contains('expanded');

        if (isFolder && isExpanded) {
            // Collapse folder
            const toggle = this.focusedNode.querySelector('.tree-node-toggle');
            if (toggle) toggle.click();
        } else {
            // Ascend to parent
            const parent = this.focusedNode.parentElement?.closest('.tree-node');
            if (parent) {
                this.focusedNode.classList.remove('keyboard-focused');
                parent.classList.add('keyboard-focused');
                this.focusedNode = parent;
                parent.querySelector('.tree-node-content')?.scrollIntoView({ block: 'nearest' });
            }
        }
    }

    selectFocused(nodes) {
        if (!this.focusedNode) return;

        const isFolder = this.focusedNode.classList.contains('folder');
        
        if (isFolder) {
            // Toggle folder
            const toggle = this.focusedNode.querySelector('.tree-node-toggle');
            if (toggle) toggle.click();
        } else {
            // Select file
            const content = this.focusedNode.querySelector('.tree-node-content');
            if (content) content.click();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Extended Keyboard Shortcuts
// ═══════════════════════════════════════════════════════════════════════════════

function setupExtendedShortcuts() {
    document.addEventListener('keydown', (e) => {
        // Don't handle if in input
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

        // View switching shortcuts
        if (!e.ctrlKey && !e.metaKey && !e.altKey) {
            switch (e.key) {
                case '1':
                    e.preventDefault();
                    document.dispatchEvent(new CustomEvent('shortcut:view-files'));
                    break;
                case '2':
                    e.preventDefault();
                    document.dispatchEvent(new CustomEvent('shortcut:view-hashes'));
                    break;
                case '3':
                    e.preventDefault();
                    document.dispatchEvent(new CustomEvent('shortcut:view-raw'));
                    break;
            }
        }
    });

    console.log('✅ Extended shortcuts initialized (1=Files, 2=Hashes, 3=Raw)');
}

// ═══════════════════════════════════════════════════════════════════════════════
// Initialize Patches
// ═══════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    // Initialize sidebar resizer
    const sidebarResizer = new SidebarResizer();
    sidebarResizer.init();
    window.sidebarResizer = sidebarResizer;

    // Initialize tree keyboard navigation
    const treeKeyNav = new TreeKeyboardNav();
    treeKeyNav.init();
    window.treeKeyNav = treeKeyNav;

    // Initialize extended shortcuts
    setupExtendedShortcuts();

    console.log('✅ v0.1.1 patches loaded');
});
