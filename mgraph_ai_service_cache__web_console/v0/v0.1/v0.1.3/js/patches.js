/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.3 Patches
   
   - Dual resize handlers (sidebar and preview panel)
   - HTML panel visibility management
   - Scroll isolation enforcement
   ═══════════════════════════════════════════════════════════════════════════════ */

// ═══════════════════════════════════════════════════════════════════════════════
// Column Resizer - Handles both resize handles
// ═══════════════════════════════════════════════════════════════════════════════

class ColumnResizer {
    constructor() {
        this.activeHandle = null;
        this.startX = 0;
        this.startWidth = 0;
        this.targetElement = null;
        this.isRight = false; // Whether resizing affects right edge
    }

    init() {
        // Handle 1: Sidebar resize
        const handle1 = document.getElementById('resize-handle-1');
        if (handle1) {
            handle1.addEventListener('mousedown', (e) => this.startResize(e, 'sidebar'));
        }

        // Handle 2: Preview panel resize
        const handle2 = document.getElementById('resize-handle-2');
        if (handle2) {
            handle2.addEventListener('mousedown', (e) => this.startResize(e, 'preview'));
        }

        // Global mouse events
        document.addEventListener('mousemove', (e) => this.onMouseMove(e));
        document.addEventListener('mouseup', () => this.stopResize());

        // Load saved widths
        this.loadWidths();

        console.log('✅ Column resizer initialized (v0.1.3)');
    }

    startResize(e, type) {
        e.preventDefault();
        this.activeHandle = type;
        this.startX = e.clientX;

        if (type === 'sidebar') {
            this.targetElement = document.getElementById('sidebar');
            this.isRight = false;
        } else if (type === 'preview') {
            this.targetElement = document.getElementById('html-preview-panel');
            this.isRight = true;
        }

        if (this.targetElement) {
            this.startWidth = this.targetElement.offsetWidth;
            document.getElementById(`resize-handle-${type === 'sidebar' ? '1' : '2'}`)?.classList.add('dragging');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        }
    }

    onMouseMove(e) {
        if (!this.activeHandle || !this.targetElement) return;

        const delta = this.isRight 
            ? this.startX - e.clientX  // Preview panel: drag left = wider
            : e.clientX - this.startX; // Sidebar: drag right = wider
        
        let newWidth = this.startWidth + delta;

        // Apply constraints
        const minWidth = parseInt(getComputedStyle(this.targetElement).minWidth) || 200;
        const maxWidth = parseInt(getComputedStyle(this.targetElement).maxWidth) || 800;
        newWidth = Math.max(minWidth, Math.min(maxWidth, newWidth));

        this.targetElement.style.width = `${newWidth}px`;
    }

    stopResize() {
        if (!this.activeHandle) return;

        // Save width
        if (this.targetElement) {
            const key = this.activeHandle === 'sidebar' ? 'cache-browser-sidebar-width' : 'cache-browser-preview-width';
            localStorage.setItem(key, this.targetElement.style.width);
        }

        // Clean up
        document.querySelectorAll('.resize-handle').forEach(h => h.classList.remove('dragging'));
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        
        this.activeHandle = null;
        this.targetElement = null;
    }

    loadWidths() {
        // Load sidebar width
        const sidebarWidth = localStorage.getItem('cache-browser-sidebar-width');
        if (sidebarWidth) {
            const sidebar = document.getElementById('sidebar');
            if (sidebar) sidebar.style.width = sidebarWidth;
        }

        // Load preview width
        const previewWidth = localStorage.getItem('cache-browser-preview-width');
        if (previewWidth) {
            const preview = document.getElementById('html-preview-panel');
            if (preview) preview.style.width = previewWidth;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTML Panel Manager - Shows/hides 3rd column based on content
// ═══════════════════════════════════════════════════════════════════════════════

class HtmlPanelManager {
    constructor() {
        this.panel = null;
        this.handle = null;
        this.htmlPanelComponent = null;
        this.isVisible = false;
    }

    init() {
        this.panel = document.getElementById('html-preview-panel');
        this.handle = document.getElementById('resize-handle-2');
        this.htmlPanelComponent = document.querySelector('html-panel');

        // Listen for close event from panel
        document.addEventListener('html-panel:close', () => {
            this.hide();
        });

        console.log('✅ HTML panel manager initialized (v0.1.3)');
    }

    show(html, path, sourceType) {
        if (!this.panel || !this.handle) return;

        // Show panel and handle
        this.panel.style.display = 'flex';
        this.panel.classList.add('visible');
        this.handle.style.display = 'block';
        this.handle.classList.add('visible');
        this.isVisible = true;

        // Dispatch event to HTML panel component
        document.dispatchEvent(new CustomEvent('html-panel:show', {
            detail: { html, path, sourceType }
        }));

        console.log('🌐 HTML panel shown for:', path);
    }

    hide() {
        if (!this.panel || !this.handle) return;

        // Hide panel and handle
        this.panel.style.display = 'none';
        this.panel.classList.remove('visible');
        this.handle.style.display = 'none';
        this.handle.classList.remove('visible');
        this.isVisible = false;

        // Clear panel content
        document.dispatchEvent(new CustomEvent('html-panel:hide'));

        console.log('🌐 HTML panel hidden');
    }

    toggle() {
        if (this.isVisible) {
            this.hide();
        }
        // Note: Don't toggle to show - needs content
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Scroll Isolation Enforcer
// ═══════════════════════════════════════════════════════════════════════════════

class ScrollIsolationEnforcer {
    init() {
        // Wait for components to be ready
        setTimeout(() => this.enforce(), 500);
        
        // Re-enforce on window resize
        window.addEventListener('resize', () => this.enforce());
    }

    enforce() {
        // Ensure app-main doesn't scroll
        const appMain = document.querySelector('.app-main');
        if (appMain) {
            appMain.style.overflow = 'hidden';
            appMain.style.position = 'relative';
        }

        // Ensure sidebar scrolls internally via file-tree
        const sidebar = document.querySelector('.sidebar');
        if (sidebar) {
            sidebar.style.overflow = 'hidden';
        }

        // Find file-tree's shadow DOM and make tree-content scrollable
        const fileTree = document.querySelector('file-tree');
        if (fileTree && fileTree.shadowRoot) {
            const treeContent = fileTree.shadowRoot.querySelector('.tree-content');
            if (treeContent) {
                treeContent.style.overflow = 'auto';
                treeContent.style.flex = '1';
                treeContent.style.minHeight = '0';
            }
            
            // Make the host scroll
            const container = fileTree.shadowRoot.querySelector('.file-tree-container');
            if (container) {
                container.style.display = 'flex';
                container.style.flexDirection = 'column';
                container.style.height = '100%';
                container.style.overflow = 'hidden';
            }
        }

        // Ensure content-viewer scrolls internally
        const contentViewer = document.querySelector('content-viewer');
        if (contentViewer && contentViewer.shadowRoot) {
            const viewerBody = contentViewer.shadowRoot.querySelector('.viewer-body');
            if (viewerBody) {
                viewerBody.style.overflow = 'auto';
                viewerBody.style.flex = '1';
                viewerBody.style.minHeight = '0';
            }
        }

        console.log('✅ Scroll isolation enforced (v0.1.3)');
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Initialize
// ═══════════════════════════════════════════════════════════════════════════════

// Global instances
window.columnResizer = new ColumnResizer();
window.htmlPanelManager = new HtmlPanelManager();
window.scrollEnforcer = new ScrollIsolationEnforcer();

document.addEventListener('DOMContentLoaded', () => {
    window.columnResizer.init();
    window.htmlPanelManager.init();
    window.scrollEnforcer.init();

    console.log('✅ v0.1.3 patches loaded');
});
