/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - HTML Viewer Component
   v0.1.2 - Inline HTML preview with source toggle
   
   Can display:
   - Raw HTML content (text files containing HTML)
   - JSON with 'html' field (extracts and displays the HTML)
   
   Events emitted:
   - html-viewer:maximize
   - html-viewer:copy
   ═══════════════════════════════════════════════════════════════════════════════ */

class HtmlViewer extends BaseComponent {
    constructor() {
        super();
        this.htmlContent = '';
        this.sourceType = 'raw'; // 'raw' | 'json-field'
        this.viewMode = 'preview'; // 'preview' | 'source' | 'split'
        this.filePath = '';
    }

    bindElements() {
        this.modeButtons = this.$$('.view-mode-btn');
        this.infoEl = this.$('#html-info');
        this.copyBtn = this.$('#html-copy-btn');
        this.maximizeBtn = this.$('#html-maximize-btn');
        this.contentEl = this.$('#html-content');
        this.previewContainer = this.$('#preview-container');
        this.sourceContainer = this.$('#source-container');
        this.iframe = this.$('#html-iframe');
        this.sourceEl = this.$('#html-source');
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

        // Maximize button
        this.addTrackedListener(this.maximizeBtn, 'click', () => this.maximize());

        // Keyboard shortcut for maximize
        this.addDocumentListener('keydown', (e) => {
            if (e.key === 'm' && !e.ctrlKey && !e.metaKey && !e.altKey) {
                if (e.target.tagName !== 'INPUT' && e.target.tagName !== 'TEXTAREA') {
                    // Only if this viewer is visible
                    if (this.offsetParent !== null) {
                        e.preventDefault();
                        this.maximize();
                    }
                }
            }
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Load HTML content
     * @param {string} html - The HTML content
     * @param {Object} options - { sourceType: 'raw'|'json-field', filePath: string }
     */
    setContent(html, options = {}) {
        this.htmlContent = html || '';
        this.sourceType = options.sourceType || 'raw';
        this.filePath = options.filePath || '';

        // Update info
        const size = new Blob([this.htmlContent]).size;
        const sizeStr = Helpers.formatBytes(size);
        const typeLabel = this.sourceType === 'json-field' ? 'from JSON .html field' : 'raw HTML';
        this.infoEl.textContent = `${sizeStr} • ${typeLabel}`;

        // Render content
        this.renderPreview();
        this.renderSource();
    }

    /**
     * Set content from JSON object with 'html' field
     */
    setFromJson(json, filePath = '') {
        if (json && typeof json.html === 'string') {
            this.setContent(json.html, { sourceType: 'json-field', filePath });
        } else {
            console.warn('JSON does not have an html field');
            this.setContent('', { sourceType: 'json-field', filePath });
        }
    }

    /**
     * Get the current HTML content
     */
    getContent() {
        return this.htmlContent;
    }

    /**
     * Clear the viewer
     */
    clear() {
        this.htmlContent = '';
        this.sourceType = 'raw';
        this.filePath = '';
        this.infoEl.textContent = '';
        this.iframe.srcdoc = '';
        this.sourceEl.textContent = '';
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

        // Update content display
        this.contentEl.classList.remove('split-view');
        
        switch (mode) {
            case 'preview':
                this.previewContainer.style.display = 'block';
                this.sourceContainer.style.display = 'none';
                break;
            case 'source':
                this.previewContainer.style.display = 'none';
                this.sourceContainer.style.display = 'block';
                break;
            case 'split':
                this.contentEl.classList.add('split-view');
                this.previewContainer.style.display = 'block';
                this.sourceContainer.style.display = 'block';
                break;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Rendering
    // ═══════════════════════════════════════════════════════════════════════════

    renderPreview() {
        // Use srcdoc for safe rendering
        this.iframe.srcdoc = this.htmlContent;
    }

    renderSource() {
        // Basic HTML syntax highlighting
        const highlighted = this.highlightHtml(this.htmlContent);
        this.sourceEl.innerHTML = highlighted;
    }

    highlightHtml(html) {
        if (!html) return '';
        
        // Escape first
        let escaped = Helpers.escapeHtml(html);
        
        // Then apply highlighting (basic)
        // Tags
        escaped = escaped.replace(/&lt;(\/?[\w-]+)/g, '&lt;<span class="tag">$1</span>');
        escaped = escaped.replace(/([\w-]+)&gt;/g, '<span class="tag">$1</span>&gt;');
        
        // Attributes
        escaped = escaped.replace(/(\s)([\w-]+)(=)/g, '$1<span class="attr-name">$2</span>$3');
        escaped = escaped.replace(/(&quot;[^&]*&quot;)/g, '<span class="attr-value">$1</span>');
        
        // Comments
        escaped = escaped.replace(/(&lt;!--[\s\S]*?--&gt;)/g, '<span class="comment">$1</span>');
        
        return escaped;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Actions
    // ═══════════════════════════════════════════════════════════════════════════

    copyHtml() {
        Helpers.copyToClipboard(this.htmlContent);
        
        // Visual feedback
        const originalText = this.copyBtn.textContent;
        this.copyBtn.textContent = '✓ Copied!';
        setTimeout(() => {
            this.copyBtn.textContent = originalText;
        }, 1500);
    }

    maximize() {
        this.emit('html-viewer:maximize', {
            html: this.htmlContent,
            sourceType: this.sourceType,
            filePath: this.filePath
        });

        // Also trigger the global modal
        window.htmlViewerModal?.show(this.htmlContent, this.filePath);
    }
}

// Register component
customElements.define('html-viewer', HtmlViewer);
console.log('✅ HtmlViewer component registered (v0.1.2)');
