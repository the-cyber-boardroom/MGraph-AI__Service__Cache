/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.2 Patches
   
   - Fix keyboard navigation (make tree focusable)
   - HTML Viewer modal manager
   - Content type detection for HTML in JSON
   ═══════════════════════════════════════════════════════════════════════════════ */

// ═══════════════════════════════════════════════════════════════════════════════
// JSON Truncation Fix
// Patches the content-viewer's formatJsonHtml to not truncate strings
// ═══════════════════════════════════════════════════════════════════════════════

class JsonTruncationFix {
    constructor() {
        this.contentViewer = null;
    }

    init() {
        // Wait for content-viewer to be ready
        const check = setInterval(() => {
            this.contentViewer = document.querySelector('content-viewer');
            if (this.contentViewer && this.contentViewer.formatJsonHtml) {
                clearInterval(check);
                this.patchFormatJsonHtml();
            }
        }, 100);

        setTimeout(() => clearInterval(check), 10000);
    }

    patchFormatJsonHtml() {
        const viewer = this.contentViewer;
        const originalFormat = viewer.formatJsonHtml.bind(viewer);

        // Override the method to remove truncation
        viewer.formatJsonHtml = function(obj, indent = 0) {
            const spaces = '  '.repeat(indent);
            
            if (obj === null) {
                return `<span class="json-null">null</span>`;
            }
            
            if (typeof obj === 'boolean') {
                return `<span class="json-boolean">${obj}</span>`;
            }
            
            if (typeof obj === 'number') {
                return `<span class="json-number">${obj}</span>`;
            }
            
            if (typeof obj === 'string') {
                // NO TRUNCATION - show full string
                const escaped = this.escapeHtml(obj);
                return `<span class="json-string">"${escaped}"</span>`;
            }
            
            if (Array.isArray(obj)) {
                if (obj.length === 0) {
                    return `<span class="json-bracket">[]</span>`;
                }
                
                const id = Helpers.generateId('json');
                let html = `<span class="json-collapsible" data-id="${id}"><span class="json-toggle">▼</span><span class="json-bracket">[</span></span>`;
                html += `<span class="json-collapsed-preview" style="display:none;"> ... ${obj.length} items ]</span>`;
                html += `<span class="json-children" data-parent="${id}">`;
                
                obj.forEach((item, i) => {
                    html += `\n${spaces}  ${this.formatJsonHtml(item, indent + 1)}`;
                    if (i < obj.length - 1) html += ',';
                });
                
                html += `\n${spaces}<span class="json-bracket">]</span></span>`;
                return html;
            }
            
            if (typeof obj === 'object') {
                const keys = Object.keys(obj);
                if (keys.length === 0) {
                    return `<span class="json-bracket">{}</span>`;
                }
                
                const id = Helpers.generateId('json');
                let html = `<span class="json-collapsible" data-id="${id}"><span class="json-toggle">▼</span><span class="json-bracket">{</span></span>`;
                html += `<span class="json-collapsed-preview" style="display:none;"> ... ${keys.length} keys }</span>`;
                html += `<span class="json-children" data-parent="${id}">`;
                
                keys.forEach((key, i) => {
                    html += `\n${spaces}  <span class="json-key">"${this.escapeHtml(key)}"</span>: ${this.formatJsonHtml(obj[key], indent + 1)}`;
                    if (i < keys.length - 1) html += ',';
                });
                
                html += `\n${spaces}<span class="json-bracket">}</span></span>`;
                return html;
            }
            
            return String(obj);
        };

        console.log('✅ JSON truncation removed - full strings will be displayed');
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Keyboard Navigation Fix
// ═══════════════════════════════════════════════════════════════════════════════

class TreeKeyboardNavFix {
    constructor() {
        this.fileTree = null;
        this.treeContainer = null;
        this.focusedNode = null;
        this.isActive = false;
    }

    init() {
        // Wait for file-tree component to be ready
        this.waitForTree();
    }

    waitForTree() {
        const check = setInterval(() => {
            this.fileTree = document.querySelector('file-tree');
            if (this.fileTree && this.fileTree.shadowRoot) {
                // Find the tree content container in shadow DOM
                this.treeContainer = this.fileTree.shadowRoot.querySelector('.tree-content') ||
                                     this.fileTree.shadowRoot.querySelector('#tree-content');
                
                if (this.treeContainer) {
                    clearInterval(check);
                    this.setupFocusableTree();
                }
            }
        }, 100);

        setTimeout(() => clearInterval(check), 10000);
    }

    setupFocusableTree() {
        // Make the tree container focusable
        this.treeContainer.setAttribute('tabindex', '0');
        this.treeContainer.style.outline = 'none';

        // Add focus event to track when tree is active
        this.treeContainer.addEventListener('focus', () => {
            this.isActive = true;
            this.treeContainer.classList.add('keyboard-active');
            console.log('🎯 Tree focused - keyboard navigation active');
        });

        this.treeContainer.addEventListener('blur', () => {
            this.isActive = false;
            this.treeContainer.classList.remove('keyboard-active');
        });

        // Click on tree should focus it
        this.treeContainer.addEventListener('click', (e) => {
            // Don't steal focus if clicking on a node (let the node handle it)
            if (!e.target.closest('.tree-node-content')) {
                this.treeContainer.focus();
            }
        });

        // Handle keyboard events on the tree container
        this.treeContainer.addEventListener('keydown', (e) => this.handleKeydown(e));

        console.log('✅ Tree keyboard navigation fix applied (v0.1.2)');
    }

    handleKeydown(e) {
        // Only handle if tree is focused
        if (!this.isActive) return;

        const nodes = Array.from(this.treeContainer.querySelectorAll('.tree-node'));
        const visibleNodes = nodes.filter(node => {
            // Check if node is visible (not inside collapsed parent)
            let parent = node.parentElement;
            while (parent && parent !== this.treeContainer) {
                if (parent.classList.contains('tree-node-children')) {
                    const parentNode = parent.closest('.tree-node');
                    if (parentNode && !parentNode.classList.contains('expanded')) {
                        return false;
                    }
                }
                parent = parent.parentElement;
            }
            return true;
        });

        if (visibleNodes.length === 0) return;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                e.stopPropagation();
                this.navigateVertical(visibleNodes, 1);
                break;
            case 'ArrowUp':
                e.preventDefault();
                e.stopPropagation();
                this.navigateVertical(visibleNodes, -1);
                break;
            case 'ArrowRight':
                e.preventDefault();
                e.stopPropagation();
                this.expandOrDescend(visibleNodes);
                break;
            case 'ArrowLeft':
                e.preventDefault();
                e.stopPropagation();
                this.collapseOrAscend(visibleNodes);
                break;
            case 'Enter':
            case ' ':
                e.preventDefault();
                e.stopPropagation();
                this.selectFocused();
                break;
        }
    }

    navigateVertical(visibleNodes, direction) {
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
            content.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        }
    }

    expandOrDescend(visibleNodes) {
        if (!this.focusedNode) return;

        const isFolder = this.focusedNode.classList.contains('folder');
        const isExpanded = this.focusedNode.classList.contains('expanded');

        if (isFolder && !isExpanded) {
            // Expand folder - click the toggle
            const toggle = this.focusedNode.querySelector('.tree-node-toggle');
            if (toggle) toggle.click();
        } else if (isFolder && isExpanded) {
            // Move to first child
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

    collapseOrAscend(visibleNodes) {
        if (!this.focusedNode) return;

        const isFolder = this.focusedNode.classList.contains('folder');
        const isExpanded = this.focusedNode.classList.contains('expanded');

        if (isFolder && isExpanded) {
            // Collapse folder
            const toggle = this.focusedNode.querySelector('.tree-node-toggle');
            if (toggle) toggle.click();
        } else {
            // Move to parent
            const parentChildren = this.focusedNode.parentElement;
            if (parentChildren && parentChildren.classList.contains('tree-node-children')) {
                const parentNode = parentChildren.closest('.tree-node');
                if (parentNode) {
                    this.focusedNode.classList.remove('keyboard-focused');
                    parentNode.classList.add('keyboard-focused');
                    this.focusedNode = parentNode;
                    parentNode.querySelector('.tree-node-content')?.scrollIntoView({ block: 'nearest' });
                }
            }
        }
    }

    selectFocused() {
        if (!this.focusedNode) return;

        const isFolder = this.focusedNode.classList.contains('folder');
        
        if (isFolder) {
            // Toggle folder
            const toggle = this.focusedNode.querySelector('.tree-node-toggle');
            if (toggle) toggle.click();
        } else {
            // Select file - click the content
            const content = this.focusedNode.querySelector('.tree-node-content');
            if (content) content.click();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTML Viewer Modal Manager
// ═══════════════════════════════════════════════════════════════════════════════

class HtmlViewerModal {
    constructor() {
        this.modal = null;
        this.titleEl = null;
        this.bodyEl = null;
        this.isShowingSource = false;
        this.currentHtml = '';
        this.currentPath = '';
    }

    init() {
        this.modal = document.getElementById('html-viewer-modal');
        this.titleEl = document.getElementById('html-viewer-modal-title');
        this.bodyEl = document.getElementById('html-viewer-modal-body');

        if (!this.modal) {
            console.warn('HTML viewer modal not found');
            return;
        }

        // Bind buttons
        document.getElementById('html-modal-toggle-source')?.addEventListener('click', () => {
            this.toggleSource();
        });

        document.getElementById('html-modal-copy')?.addEventListener('click', () => {
            Helpers.copyToClipboard(this.currentHtml);
        });

        document.getElementById('html-modal-close')?.addEventListener('click', () => {
            this.hide();
        });

        // ESC to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal.style.display !== 'none') {
                this.hide();
            }
        });

        console.log('✅ HTML viewer modal initialized');
    }

    show(html, path = '') {
        this.currentHtml = html;
        this.currentPath = path;
        this.isShowingSource = false;

        // Update title
        this.titleEl.textContent = path ? `HTML Preview: ${path.split('/').pop()}` : 'HTML Preview';

        // Render preview
        this.renderPreview();

        // Show modal
        this.modal.style.display = 'flex';

        // Update toggle button
        document.getElementById('html-modal-toggle-icon').textContent = '📄';
    }

    hide() {
        this.modal.style.display = 'none';
        this.bodyEl.innerHTML = '';
    }

    toggleSource() {
        this.isShowingSource = !this.isShowingSource;
        
        if (this.isShowingSource) {
            this.renderSource();
            document.getElementById('html-modal-toggle-icon').textContent = '🌐';
        } else {
            this.renderPreview();
            document.getElementById('html-modal-toggle-icon').textContent = '📄';
        }
    }

    renderPreview() {
        this.bodyEl.innerHTML = `<iframe sandbox="allow-same-origin" style="width: 100%; height: 100%; border: none; background: white;"></iframe>`;
        this.bodyEl.querySelector('iframe').srcdoc = this.currentHtml;
    }

    renderSource() {
        const escaped = Helpers.escapeHtml(this.currentHtml);
        this.bodyEl.innerHTML = `
            <div class="html-source" style="padding: 16px; overflow: auto; height: 100%;">
                <pre style="margin: 0; white-space: pre-wrap; font-family: monospace; font-size: 13px; line-height: 1.5;">${escaped}</pre>
            </div>
        `;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTML Content Detection Helper
// ═══════════════════════════════════════════════════════════════════════════════

const HtmlDetector = {
    /**
     * Check if JSON object contains an 'html' field
     */
    hasHtmlField(obj) {
        return obj && typeof obj === 'object' && typeof obj.html === 'string';
    },

    /**
     * Check if string content looks like HTML
     */
    isHtmlContent(str) {
        if (typeof str !== 'string') return false;
        const trimmed = str.trim();
        return trimmed.startsWith('<!DOCTYPE') || 
               trimmed.startsWith('<!doctype') ||
               trimmed.startsWith('<html') ||
               trimmed.startsWith('<HTML') ||
               (trimmed.startsWith('<') && trimmed.includes('</'));
    },

    /**
     * Extract HTML from various sources
     */
    extractHtml(content, contentType) {
        // If it's already HTML text
        if (typeof content === 'string' && this.isHtmlContent(content)) {
            return { html: content, sourceType: 'raw' };
        }

        // If it's JSON with html field
        if (typeof content === 'object' && this.hasHtmlField(content)) {
            return { html: content.html, sourceType: 'json-field' };
        }

        // If it's a JSON string, try to parse
        if (typeof content === 'string') {
            try {
                const parsed = JSON.parse(content);
                if (this.hasHtmlField(parsed)) {
                    return { html: parsed.html, sourceType: 'json-field' };
                }
            } catch (e) {
                // Not JSON, ignore
            }
        }

        return null;
    }
};

// Make globally available
window.HtmlDetector = HtmlDetector;

// ═══════════════════════════════════════════════════════════════════════════════
// Initialize
// ═══════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    // Initialize JSON truncation fix
    const jsonFix = new JsonTruncationFix();
    jsonFix.init();
    window.jsonTruncationFix = jsonFix;

    // Initialize keyboard navigation fix
    const treeKeyNav = new TreeKeyboardNavFix();
    treeKeyNav.init();
    window.treeKeyNavFix = treeKeyNav;

    // Initialize HTML modal
    const htmlModal = new HtmlViewerModal();
    htmlModal.init();
    window.htmlViewerModal = htmlModal;

    console.log('✅ v0.1.2 patches loaded');
});
