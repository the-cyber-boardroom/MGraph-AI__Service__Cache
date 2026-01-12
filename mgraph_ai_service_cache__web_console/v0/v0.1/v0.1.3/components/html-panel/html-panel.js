/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - HTML Panel Component
   v0.1.3 - Standalone HTML preview panel (3rd column)
   
   Events emitted:
   - html-panel:close - User wants to close the panel
   - html-panel:open-tab - Open HTML in new browser tab
   
   Events listened:
   - html-panel:show - Show HTML content { html, path, sourceType }
   - html-panel:hide - Hide the panel
   ═══════════════════════════════════════════════════════════════════════════════ */

class HtmlPanel extends BaseComponent {
    constructor() {
        super();
        this.htmlContent = '';
        this.filePath = '';
        this.sourceType = 'raw';
        this.viewMode = 'preview';
    }

    bindElements() {
        this.titleEl = this.$('#panel-title');
        this.copyBtn = this.$('#copy-btn');
        this.openBtn = this.$('#open-btn');
        this.closeBtn = this.$('#close-btn');
        this.modeButtons = this.$$('.mode-btn');
        this.sizeEl = this.$('#html-size');
        this.bodyEl = this.$('#panel-body');
        this.iframe = this.$('#preview-iframe');
        this.sourceView = this.$('#source-view');
        this.sourceCode = this.$('#source-code');
        this.emptyState = this.$('#empty-state');
    }

    setupEventListeners() {
        // Mode toggle buttons
        this.modeButtons.forEach(btn => {
            this.addTrackedListener(btn, 'click', () => {
                this.setViewMode(btn.dataset.mode);
            });
        });

        // Copy button
        this.addTrackedListener(this.copyBtn, 'click', () => this.copyHtml());

        // Open in new tab
        this.addTrackedListener(this.openBtn, 'click', () => this.openInNewTab());

        // Close button
        this.addTrackedListener(this.closeBtn, 'click', () => {
            this.emit('html-panel:close');
        });

        // Listen for show/hide events
        this.addDocumentListener('html-panel:show', (e) => {
            this.showContent(e.detail.html, e.detail.path, e.detail.sourceType);
        });

        this.addDocumentListener('html-panel:hide', () => {
            this.clear();
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    showContent(html, path = '', sourceType = 'raw') {
        this.htmlContent = html || '';
        this.filePath = path;
        this.sourceType = sourceType;

        // Update title
        const fileName = path ? path.split('/').pop() : 'HTML Preview';
        this.titleEl.textContent = fileName;
        this.titleEl.title = path;

        // Update size
        const size = new Blob([this.htmlContent]).size;
        this.sizeEl.textContent = Helpers.formatBytes(size);

        // Hide empty state, show content
        this.emptyState.style.display = 'none';
        this.iframe.style.display = this.viewMode === 'preview' ? 'block' : 'none';
        this.sourceView.style.display = this.viewMode === 'source' ? 'block' : 'none';

        // Render content
        this.renderPreview();
        this.renderSource();
    }

    clear() {
        this.htmlContent = '';
        this.filePath = '';
        this.sourceType = 'raw';
        this.titleEl.textContent = 'HTML Preview';
        this.sizeEl.textContent = '';
        this.iframe.srcdoc = '';
        this.sourceCode.textContent = '';
        
        // Show empty state
        this.emptyState.style.display = 'flex';
        this.iframe.style.display = 'none';
        this.sourceView.style.display = 'none';
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // View Modes
    // ═══════════════════════════════════════════════════════════════════════════

    setViewMode(mode) {
        this.viewMode = mode;

        // Update button states
        this.modeButtons.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.mode === mode);
        });

        // Show/hide views
        if (this.htmlContent) {
            this.iframe.style.display = mode === 'preview' ? 'block' : 'none';
            this.sourceView.style.display = mode === 'source' ? 'block' : 'none';
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Rendering
    // ═══════════════════════════════════════════════════════════════════════════

    renderPreview() {
        this.iframe.srcdoc = this.htmlContent;
    }

    renderSource() {
        // Basic syntax highlighting
        const escaped = Helpers.escapeHtml(this.htmlContent);
        this.sourceCode.innerHTML = escaped;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Actions
    // ═══════════════════════════════════════════════════════════════════════════

    copyHtml() {
        Helpers.copyToClipboard(this.htmlContent);
        
        // Visual feedback
        const originalText = this.copyBtn.textContent;
        this.copyBtn.textContent = '✓';
        setTimeout(() => {
            this.copyBtn.textContent = originalText;
        }, 1500);
    }

    openInNewTab() {
        // Create a blob URL and open it
        const blob = new Blob([this.htmlContent], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        window.open(url, '_blank');
        
        // Clean up after a delay
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
}

// Register component
customElements.define('html-panel', HtmlPanel);
console.log('✅ HtmlPanel component registered (v0.1.3)');
