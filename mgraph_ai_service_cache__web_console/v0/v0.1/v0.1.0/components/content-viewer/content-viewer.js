/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Content Viewer Component
   v0.1.0 - Multi-format content viewer with maximize mode
   
   Supports: JSON, Text, HTML, Images, Binary
   
   Events emitted:
   - content-loaded: { path, type, size }
   
   Events listened:
   - file-selected
   - shortcut:maximize-toggle
   - shortcut:copy
   - shortcut:reload
   - shortcut:escape
   - shortcut:tab-next
   - shortcut:tab-prev
   ═══════════════════════════════════════════════════════════════════════════════ */

class ContentViewer extends BaseComponent {
    constructor() {
        super();
        this.currentPath = null;
        this.currentNamespace = null;
        this.currentType = null;
        this.currentContent = null;
        this.rawContent = null;
        this.isMaximized = false;
        this.activeTab = 'formatted';
        this.jsonExpanded = new Set();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    bindElements() {
        // Header
        this.fileIcon = this.$('#file-icon');
        this.fileName = this.$('#file-name');
        this.filePath = this.$('#file-path');
        this.copyBtn = this.$('#copy-btn');
        this.reloadBtn = this.$('#reload-btn');
        this.maximizeBtn = this.$('#maximize-btn');

        // Tabs
        this.tabs = this.$$('.viewer-tab');

        // States
        this.emptyState = this.$('#empty-state');
        this.loadingState = this.$('#loading-state');
        this.errorState = this.$('#error-state');
        this.errorMessage = this.$('#error-message');
        this.errorRetryBtn = this.$('#error-retry-btn');

        // Content containers
        this.formattedContent = this.$('#formatted-content');
        this.rawContentEl = this.$('#raw-content');
        this.infoContent = this.$('#info-content');
        this.rawText = this.$('#raw-text');
        this.infoGrid = this.$('#info-grid');

        // Viewers
        this.jsonViewer = this.$('#json-viewer');
        this.jsonContent = this.$('#json-content');
        this.jsonStats = this.$('#json-stats');
        this.jsonExpandAll = this.$('#json-expand-all');
        this.jsonCollapseAll = this.$('#json-collapse-all');

        this.textViewer = this.$('#text-viewer');
        this.textContent = this.$('#text-content');
        this.textStats = this.$('#text-stats');
        this.wrapLinesCheckbox = this.$('#wrap-lines');

        this.htmlViewer = this.$('#html-viewer');
        this.htmlPreview = this.$('#html-preview');
        this.htmlIframe = this.$('#html-iframe');
        this.htmlSource = this.$('#html-source');
        this.htmlPreviewBtn = this.$('#html-preview-btn');
        this.htmlSourceBtn = this.$('#html-source-btn');

        this.imageViewer = this.$('#image-viewer');
        this.imageContainer = this.$('#image-container');
        this.imageElement = this.$('#image-element');
        this.imageStats = this.$('#image-stats');

        this.binaryViewer = this.$('#binary-viewer');
        this.binarySize = this.$('#binary-size');
        this.binaryDownloadBtn = this.$('#binary-download-btn');

        // Maximize overlay
        this.maximizeOverlay = this.$('#maximize-overlay');
        this.minimizeBtn = this.$('#minimize-btn');
        this.maxFilename = this.$('#max-filename');
        this.maxCopyBtn = this.$('#max-copy-btn');
        this.maxReloadBtn = this.$('#max-reload-btn');
        this.maximizeContent = this.$('#maximize-content');
    }

    setupEventListeners() {
        // Buttons
        this.addTrackedListener(this.copyBtn, 'click', () => this.copyContent());
        this.addTrackedListener(this.reloadBtn, 'click', () => this.reload());
        this.addTrackedListener(this.maximizeBtn, 'click', () => this.toggleMaximize());
        this.addTrackedListener(this.errorRetryBtn, 'click', () => this.reload());

        // Tabs
        this.tabs.forEach(tab => {
            this.addTrackedListener(tab, 'click', () => this.switchTab(tab.dataset.tab));
        });

        // JSON controls
        this.addTrackedListener(this.jsonExpandAll, 'click', () => this.expandAllJson());
        this.addTrackedListener(this.jsonCollapseAll, 'click', () => this.collapseAllJson());

        // Text wrap toggle
        this.addTrackedListener(this.wrapLinesCheckbox, 'change', (e) => {
            this.textContent.classList.toggle('no-wrap', !e.target.checked);
        });

        // HTML mode toggle
        this.addTrackedListener(this.htmlPreviewBtn, 'click', () => this.setHtmlMode('preview'));
        this.addTrackedListener(this.htmlSourceBtn, 'click', () => this.setHtmlMode('source'));

        // Maximize overlay
        this.addTrackedListener(this.minimizeBtn, 'click', () => this.toggleMaximize());
        this.addTrackedListener(this.maxCopyBtn, 'click', () => this.copyContent());
        this.addTrackedListener(this.maxReloadBtn, 'click', () => this.reload());

        // Document events
        this.addDocumentListener('file-selected', (e) => this.loadFile(e.detail));
        this.addDocumentListener('shortcut:maximize-toggle', () => this.toggleMaximize());
        this.addDocumentListener('shortcut:copy', () => this.copyContent());
        this.addDocumentListener('shortcut:reload', () => this.reload());
        this.addDocumentListener('shortcut:escape', () => {
            if (this.isMaximized) this.toggleMaximize();
        });
        this.addDocumentListener('shortcut:tab-next', () => this.nextTab());
        this.addDocumentListener('shortcut:tab-prev', () => this.prevTab());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // File Loading
    // ═══════════════════════════════════════════════════════════════════════════

    async loadFile(detail) {
        const { path, type, namespace } = detail;
        
        this.currentPath = path;
        this.currentNamespace = namespace;
        this.currentType = type;

        // Update header
        const filename = Helpers.getFilename(path);
        this.fileName.textContent = filename;
        this.filePath.textContent = path;
        this.fileIcon.textContent = this.getTypeIcon(type);

        // Enable buttons
        this.copyBtn.disabled = false;
        this.reloadBtn.disabled = false;
        this.maximizeBtn.disabled = false;

        // Show loading
        this.showState('loading');

        try {
            // Fetch content
            const content = await cacheApiClient.getFileJson(path);
            this.rawContent = typeof content === 'string' ? content : JSON.stringify(content, null, 2);
            this.currentContent = content;

            // Detect content type and render
            const contentType = this.detectContentType(content, filename);
            this.renderContent(content, contentType);
            this.renderRawContent();
            this.renderInfo(path, contentType);

            // Show formatted tab
            this.showState('content');
            this.switchTab('formatted');

            // Emit event
            this.emit('content-loaded', { 
                path, 
                type: contentType, 
                size: this.rawContent.length 
            });

        } catch (error) {
            console.error('Failed to load file:', error);
            this.errorMessage.textContent = error.message || 'Failed to load content';
            this.showState('error');
        }
    }

    reload() {
        if (this.currentPath && this.currentNamespace) {
            this.loadFile({
                path: this.currentPath,
                type: this.currentType,
                namespace: this.currentNamespace
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Content Rendering
    // ═══════════════════════════════════════════════════════════════════════════

    detectContentType(content, filename) {
        const ext = Helpers.getExtension(filename);
        
        // Check by extension first
        if (ext === 'json' || ext === 'config' || ext === 'metadata') return 'json';
        if (ext === 'html' || ext === 'htm') return 'html';
        if (ext === 'txt' || ext === 'text' || ext === 'md') return 'text';
        if (['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'ico'].includes(ext)) return 'image';
        if (['bin', 'binary', 'dat'].includes(ext)) return 'binary';

        // Check content
        if (typeof content === 'object') return 'json';
        if (typeof content === 'string') {
            const trimmed = content.trim();
            if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
                try {
                    JSON.parse(trimmed);
                    return 'json';
                } catch {}
            }
            if (trimmed.startsWith('<!DOCTYPE') || trimmed.startsWith('<html')) {
                return 'html';
            }
        }

        return 'text';
    }

    renderContent(content, type) {
        // Hide all viewers
        this.jsonViewer.style.display = 'none';
        this.textViewer.style.display = 'none';
        this.htmlViewer.style.display = 'none';
        this.imageViewer.style.display = 'none';
        this.binaryViewer.style.display = 'none';

        switch (type) {
            case 'json':
                this.renderJson(content);
                break;
            case 'html':
                this.renderHtml(content);
                break;
            case 'image':
                this.renderImage(content);
                break;
            case 'binary':
                this.renderBinary(content);
                break;
            default:
                this.renderText(content);
        }
    }

    renderJson(content) {
        this.jsonViewer.style.display = 'flex';
        
        const obj = typeof content === 'string' ? JSON.parse(content) : content;
        const html = this.formatJsonHtml(obj);
        this.jsonContent.innerHTML = html;

        // Stats
        const stats = this.getJsonStats(obj);
        this.jsonStats.textContent = `${stats.keys} keys • ${stats.depth} levels deep`;

        // Bind collapsible events
        this.bindJsonToggle();
    }

    formatJsonHtml(obj, indent = 0) {
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
            const escaped = this.escapeHtml(obj);
            const truncated = escaped.length > 500 ? escaped.substring(0, 500) + '...' : escaped;
            return `<span class="json-string">"${truncated}"</span>`;
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
    }

    getJsonStats(obj, depth = 0) {
        let keys = 0;
        let maxDepth = depth;
        
        if (Array.isArray(obj)) {
            obj.forEach(item => {
                const sub = this.getJsonStats(item, depth + 1);
                keys += sub.keys;
                maxDepth = Math.max(maxDepth, sub.depth);
            });
        } else if (obj && typeof obj === 'object') {
            keys = Object.keys(obj).length;
            Object.values(obj).forEach(value => {
                const sub = this.getJsonStats(value, depth + 1);
                keys += sub.keys;
                maxDepth = Math.max(maxDepth, sub.depth);
            });
        }
        
        return { keys, depth: maxDepth };
    }

    bindJsonToggle() {
        this.$$('.json-collapsible').forEach(el => {
            this.addTrackedListener(el, 'click', (e) => {
                e.stopPropagation();
                const id = el.dataset.id;
                const toggle = el.querySelector('.json-toggle');
                const children = this.$(`.json-children[data-parent="${id}"]`);
                const preview = el.nextElementSibling;
                
                if (toggle.classList.contains('collapsed')) {
                    toggle.classList.remove('collapsed');
                    toggle.textContent = '▼';
                    children.classList.remove('json-hidden');
                    if (preview) preview.style.display = 'none';
                } else {
                    toggle.classList.add('collapsed');
                    toggle.textContent = '▶';
                    children.classList.add('json-hidden');
                    if (preview) preview.style.display = 'inline';
                }
            });
        });
    }

    expandAllJson() {
        this.$$('.json-toggle').forEach(toggle => {
            toggle.classList.remove('collapsed');
            toggle.textContent = '▼';
        });
        this.$$('.json-children').forEach(el => el.classList.remove('json-hidden'));
        this.$$('.json-collapsed-preview').forEach(el => el.style.display = 'none');
    }

    collapseAllJson() {
        this.$$('.json-toggle').forEach(toggle => {
            toggle.classList.add('collapsed');
            toggle.textContent = '▶';
        });
        this.$$('.json-children').forEach(el => el.classList.add('json-hidden'));
        this.$$('.json-collapsed-preview').forEach(el => el.style.display = 'inline');
    }

    renderText(content) {
        this.textViewer.style.display = 'flex';
        const text = typeof content === 'string' ? content : JSON.stringify(content, null, 2);
        this.textContent.textContent = text;
        
        const lines = text.split('\n').length;
        const chars = text.length;
        this.textStats.textContent = `${lines} lines • ${Helpers.formatNumber(chars)} chars`;
    }

    renderHtml(content) {
        this.htmlViewer.style.display = 'flex';
        const html = typeof content === 'string' ? content : JSON.stringify(content);
        
        // Set iframe content
        this.htmlIframe.srcdoc = html;
        
        // Format source
        this.htmlSource.innerHTML = Helpers.formatJson(html);
        
        this.setHtmlMode('preview');
    }

    setHtmlMode(mode) {
        this.htmlPreviewBtn.classList.toggle('active', mode === 'preview');
        this.htmlSourceBtn.classList.toggle('active', mode === 'source');
        this.htmlPreview.style.display = mode === 'preview' ? 'block' : 'none';
        this.htmlSource.style.display = mode === 'source' ? 'block' : 'none';
    }

    renderImage(content) {
        this.imageViewer.style.display = 'flex';
        
        // Handle base64 or URL
        if (typeof content === 'string') {
            if (content.startsWith('data:image/') || content.startsWith('http')) {
                this.imageElement.src = content;
            } else {
                // Try as base64
                this.imageElement.src = `data:image/png;base64,${content}`;
            }
        }
        
        this.imageElement.onload = () => {
            this.imageStats.textContent = `${this.imageElement.naturalWidth} × ${this.imageElement.naturalHeight}`;
        };
    }

    renderBinary(content) {
        this.binaryViewer.style.display = 'flex';
        const size = typeof content === 'string' ? content.length : 0;
        this.binarySize.textContent = Helpers.formatBytes(size);
    }

    renderRawContent() {
        this.rawText.textContent = this.rawContent || '';
    }

    renderInfo(path, type) {
        const parsed = Helpers.parseFilePath(path);
        
        let html = `
            <span class="info-label">Path:</span>
            <span class="info-value">${this.escapeHtml(path)}</span>
            
            <span class="info-label">Type:</span>
            <span class="info-value highlight">${type}</span>
            
            <span class="info-label">Size:</span>
            <span class="info-value">${Helpers.formatBytes(this.rawContent?.length || 0)}</span>
            
            <span class="info-label">Namespace:</span>
            <span class="info-value">${this.escapeHtml(parsed.namespace || '-')}</span>
        `;

        if (parsed.strategy) {
            html += `
                <span class="info-label">Strategy:</span>
                <span class="info-value">${this.escapeHtml(parsed.strategy)}</span>
            `;
        }

        if (parsed.hash) {
            html += `
                <span class="info-label">Hash:</span>
                <span class="info-value highlight">${this.escapeHtml(parsed.hash)}</span>
            `;
        }

        this.infoGrid.innerHTML = html;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tabs
    // ═══════════════════════════════════════════════════════════════════════════

    switchTab(tab) {
        this.activeTab = tab;
        
        this.tabs.forEach(t => {
            t.classList.toggle('active', t.dataset.tab === tab);
        });

        this.formattedContent.style.display = tab === 'formatted' ? 'block' : 'none';
        this.rawContentEl.style.display = tab === 'raw' ? 'block' : 'none';
        this.infoContent.style.display = tab === 'info' ? 'block' : 'none';
    }

    nextTab() {
        const tabOrder = ['formatted', 'raw', 'info'];
        const currentIndex = tabOrder.indexOf(this.activeTab);
        const nextIndex = (currentIndex + 1) % tabOrder.length;
        this.switchTab(tabOrder[nextIndex]);
    }

    prevTab() {
        const tabOrder = ['formatted', 'raw', 'info'];
        const currentIndex = tabOrder.indexOf(this.activeTab);
        const prevIndex = (currentIndex - 1 + tabOrder.length) % tabOrder.length;
        this.switchTab(tabOrder[prevIndex]);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Maximize Mode
    // ═══════════════════════════════════════════════════════════════════════════

    toggleMaximize() {
        this.isMaximized = !this.isMaximized;
        
        if (this.isMaximized) {
            this.maximizeOverlay.style.display = 'flex';
            this.maxFilename.textContent = Helpers.getFilename(this.currentPath || '');
            
            // Clone content
            const content = this.rawContent || '';
            const contentType = this.detectContentType(this.currentContent, this.currentPath);
            
            if (contentType === 'json') {
                this.maximizeContent.innerHTML = `<pre class="json-content">${this.formatJsonHtml(this.currentContent)}</pre>`;
            } else {
                this.maximizeContent.innerHTML = `<pre>${this.escapeHtml(content)}</pre>`;
            }
            
            document.body.style.overflow = 'hidden';
        } else {
            this.maximizeOverlay.style.display = 'none';
            document.body.style.overflow = '';
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Actions
    // ═══════════════════════════════════════════════════════════════════════════

    async copyContent() {
        if (!this.rawContent) return;
        
        const success = await Helpers.copyToClipboard(this.rawContent);
        
        // Visual feedback
        const btn = this.isMaximized ? this.maxCopyBtn : this.copyBtn;
        const originalText = btn.textContent;
        btn.textContent = success ? '✓ Copied!' : '✗ Failed';
        setTimeout(() => {
            btn.textContent = originalText;
        }, 1500);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // State Management
    // ═══════════════════════════════════════════════════════════════════════════

    showState(state) {
        this.emptyState.style.display = state === 'empty' ? 'flex' : 'none';
        this.loadingState.style.display = state === 'loading' ? 'flex' : 'none';
        this.errorState.style.display = state === 'error' ? 'flex' : 'none';
        this.formattedContent.style.display = state === 'content' && this.activeTab === 'formatted' ? 'block' : 'none';
        this.rawContentEl.style.display = state === 'content' && this.activeTab === 'raw' ? 'block' : 'none';
        this.infoContent.style.display = state === 'content' && this.activeTab === 'info' ? 'block' : 'none';
    }

    getTypeIcon(type) {
        const icons = {
            json: '📄',
            config: '⚙️',
            metadata: '🏷️',
            text: '📝',
            html: '🌐',
            image: '🖼️',
            binary: '📦',
            file: '📄',
            folder: '📁'
        };
        return icons[type] || '📄';
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    clear() {
        this.currentPath = null;
        this.currentNamespace = null;
        this.currentContent = null;
        this.rawContent = null;
        
        this.fileName.textContent = 'No file selected';
        this.filePath.textContent = '';
        this.copyBtn.disabled = true;
        this.reloadBtn.disabled = true;
        this.maximizeBtn.disabled = true;
        
        this.showState('empty');
    }
}

// Register component
customElements.define('content-viewer', ContentViewer);
console.log('✅ ContentViewer component registered');
