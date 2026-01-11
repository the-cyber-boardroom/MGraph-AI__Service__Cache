/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Files View Component
   v0.1.1 - List cache entries by file_id with detail panel
   
   Events emitted:
   - file-detail-loaded: { cacheId, refs }
   
   Events listened:
   - namespace-changed
   - view-changed
   ═══════════════════════════════════════════════════════════════════════════════ */

class FilesView extends BaseComponent {
    constructor() {
        super();
        this.namespace = null;
        this.fileIds = [];
        this.filteredIds = [];
        this.selectedId = null;
        this.focusedIndex = -1;
        this.detailData = null;
        this.activeDetailTab = 'content';
    }

    bindElements() {
        this.filterInput = this.$('#files-filter');
        this.statsEl = this.$('#files-stats');
        this.filesList = this.$('#files-list');
        this.filesItems = this.$('#files-items');
        this.loadingEl = this.$('#files-loading');
        this.emptyEl = this.$('#files-empty');
        this.errorEl = this.$('#files-error');
        this.errorMessage = this.$('#files-error-message');
        this.retryBtn = this.$('#files-retry-btn');
        this.detailEmpty = this.$('#detail-empty');
        this.detailContent = this.$('#detail-content');
    }

    setupEventListeners() {
        // Filter input
        this.addTrackedListener(this.filterInput, 'input', 
            Helpers.debounce(() => this.applyFilter(), 150)
        );

        // Retry button
        this.addTrackedListener(this.retryBtn, 'click', () => this.loadFiles());

        // Namespace changes
        this.addDocumentListener('namespace-changed', (e) => {
            this.namespace = e.detail.namespace;
            this.loadFiles();
        });

        // View changes - load data when switching to files view
        this.addDocumentListener('view-changed', (e) => {
            if (e.detail.view === 'files' && this.namespace && this.fileIds.length === 0) {
                this.loadFiles();
            }
        });

        // Keyboard navigation
        this.addDocumentListener('keydown', (e) => this.handleKeydown(e));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Data Loading
    // ═══════════════════════════════════════════════════════════════════════════

    async loadFiles() {
        if (!this.namespace) return;

        this.showState('loading');

        try {
            this.fileIds = await cacheApiClient.getFileIds(this.namespace);
            this.filteredIds = [...this.fileIds];
            this.updateStats();
            this.renderFileList();
            
            if (this.fileIds.length === 0) {
                this.showState('empty');
            } else {
                this.showState('content');
            }
        } catch (error) {
            console.error('Failed to load files:', error);
            this.errorMessage.textContent = error.message || 'Failed to load files';
            this.showState('error');
        }
    }

    async loadFileDetail(cacheId) {
        if (!this.namespace || !cacheId) return;

        this.selectedId = cacheId;
        this.updateSelection();

        try {
            // Load full refs data
            const refs = await cacheApiClient.retrieveRefsAll(this.namespace, cacheId);
            this.detailData = refs;
            this.renderDetail(refs);
            
            this.emit('file-detail-loaded', { cacheId, refs });
        } catch (error) {
            console.error('Failed to load file detail:', error);
            this.renderDetailError(error.message);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Rendering
    // ═══════════════════════════════════════════════════════════════════════════

    renderFileList() {
        this.filesItems.innerHTML = '';

        this.filteredIds.forEach((id, index) => {
            const item = document.createElement('div');
            item.className = 'file-item';
            item.dataset.id = id;
            item.dataset.index = index;
            
            item.innerHTML = `
                <span class="file-item-icon">📄</span>
                <div class="file-item-content">
                    <div class="file-item-id">${this.escapeHtml(id)}</div>
                    <div class="file-item-meta">
                        <span class="file-item-badge strategy-unknown">loading...</span>
                    </div>
                </div>
            `;

            item.addEventListener('click', () => {
                this.focusedIndex = index;
                this.loadFileDetail(id);
            });

            this.filesItems.appendChild(item);
        });
    }

    renderDetail(refs) {
        const byId = refs.by_id || {};
        const details = refs.details || {};

        this.detailEmpty.style.display = 'none';
        this.detailContent.style.display = 'flex';
        this.detailContent.style.flexDirection = 'column';

        // Extract metadata and config from details
        const metadataPath = (byId.all_paths?.data || []).find(p => p.endsWith('.metadata'));
        const configPath = (byId.all_paths?.data || []).find(p => p.endsWith('.config'));
        const metadata = details[metadataPath] || {};
        const config = details[configPath] || {};

        // Get key path for key_based strategy
        const keyPath = byId.strategy === 'key_based' ? this.extractKeyPath(byId) : null;

        this.detailContent.innerHTML = `
            <div class="detail-header">
                <span class="file-item-icon">📄</span>
                <span class="detail-file-id" title="${this.escapeHtml(byId.cache_id)}">${this.escapeHtml(byId.cache_id)}</span>
                <span class="file-item-badge strategy-${byId.strategy || 'unknown'}">${byId.strategy || 'unknown'}</span>
                <div class="detail-actions">
                    <button class="btn btn-ghost btn-sm" id="detail-copy-btn" title="Copy ID">📋</button>
                    <button class="btn btn-ghost btn-sm" id="detail-view-raw-btn" title="View in Raw">📁</button>
                </div>
            </div>
            
            ${keyPath ? `<div class="key-path-display">${this.escapeHtml(keyPath)}</div>` : ''}
            
            <div class="detail-tabs">
                <button class="detail-tab ${this.activeDetailTab === 'content' ? 'active' : ''}" data-tab="content">Content</button>
                <button class="detail-tab ${this.activeDetailTab === 'metadata' ? 'active' : ''}" data-tab="metadata">Metadata</button>
                <button class="detail-tab ${this.activeDetailTab === 'config' ? 'active' : ''}" data-tab="config">Config</button>
                <button class="detail-tab ${this.activeDetailTab === 'refs' ? 'active' : ''}" data-tab="refs">Refs</button>
            </div>
            
            <div class="detail-body" id="detail-body">
                ${this.renderDetailTab(this.activeDetailTab, { byId, metadata, config, details })}
            </div>
        `;

        // Bind tab clicks
        this.detailContent.querySelectorAll('.detail-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                this.activeDetailTab = tab.dataset.tab;
                this.renderDetail(refs);
            });
        });

        // Bind actions
        this.detailContent.querySelector('#detail-copy-btn')?.addEventListener('click', () => {
            Helpers.copyToClipboard(byId.cache_id);
        });

        this.detailContent.querySelector('#detail-view-raw-btn')?.addEventListener('click', () => {
            // Switch to raw view and select this file
            const contentPath = byId.file_paths?.content_files?.[0];
            if (contentPath) {
                document.dispatchEvent(new CustomEvent('file-selected', {
                    detail: { path: contentPath, namespace: this.namespace, type: 'json' },
                    bubbles: true
                }));
                document.querySelector('view-tabs')?.switchView('raw');
            }
        });

        // Update the list item with actual metadata
        this.updateFileItemMeta(byId.cache_id, byId, metadata);
    }

    renderDetailTab(tab, data) {
        const { byId, metadata, config, details } = data;

        switch (tab) {
            case 'content':
                return this.renderContentTab(byId, details);
            case 'metadata':
                return this.renderMetadataTab(metadata);
            case 'config':
                return this.renderConfigTab(config);
            case 'refs':
                return this.renderRefsTab(byId, details);
            default:
                return '<div>Unknown tab</div>';
        }
    }

    renderContentTab(byId, details) {
        const contentPath = byId.file_paths?.content_files?.[0];
        
        return `
            <div style="margin-bottom: var(--spacing-md);">
                <div style="font-size: var(--font-size-xs); color: var(--color-text-muted); margin-bottom: var(--spacing-xs);">
                    Content Path:
                </div>
                <code style="font-size: var(--font-size-xs); color: var(--color-accent-primary); word-break: break-all;">
                    ${this.escapeHtml(contentPath || 'N/A')}
                </code>
            </div>
            <div style="font-size: var(--font-size-xs); color: var(--color-text-muted); margin-bottom: var(--spacing-xs);">
                Click "View in Raw" (📁) to see the actual content
            </div>
            <div class="info-grid" style="margin-top: var(--spacing-md);">
                <span class="info-label">Hash:</span>
                <span class="info-value highlight">${this.escapeHtml(byId.cache_hash || 'N/A')}</span>
                
                <span class="info-label">Type:</span>
                <span class="info-value">${this.escapeHtml(byId.file_type || 'N/A')}</span>
                
                <span class="info-label">Strategy:</span>
                <span class="info-value">${this.escapeHtml(byId.strategy || 'N/A')}</span>
                
                <span class="info-label">Stored:</span>
                <span class="info-value">${byId.timestamp ? Helpers.formatDate(byId.timestamp) : 'N/A'}</span>
            </div>
        `;
    }

    renderMetadataTab(metadata) {
        if (!metadata || Object.keys(metadata).length === 0) {
            return '<div style="color: var(--color-text-muted);">No metadata available</div>';
        }

        return `<pre class="json-content">${this.escapeHtml(JSON.stringify(metadata, null, 2))}</pre>`;
    }

    renderConfigTab(config) {
        if (!config || Object.keys(config).length === 0) {
            return '<div style="color: var(--color-text-muted);">No config available</div>';
        }

        return `<pre class="json-content">${this.escapeHtml(JSON.stringify(config, null, 2))}</pre>`;
    }

    renderRefsTab(byId, details) {
        const allPaths = byId.all_paths || {};
        
        let html = '';

        // Data files
        if (allPaths.data?.length) {
            html += `
                <div class="refs-section">
                    <div class="refs-title">📁 Data Files (${allPaths.data.length})</div>
                    <div class="refs-list">
                        ${allPaths.data.map(p => `
                            <div class="ref-item">
                                <span class="ref-item-icon">${this.getFileIcon(p)}</span>
                                <span class="ref-item-path">${this.escapeHtml(p)}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }

        // By-ID refs
        if (allPaths.by_id?.length) {
            html += `
                <div class="refs-section">
                    <div class="refs-title">🔗 By-ID Refs</div>
                    <div class="refs-list">
                        ${allPaths.by_id.map(p => `
                            <div class="ref-item">
                                <span class="ref-item-icon">📎</span>
                                <span class="ref-item-path">${this.escapeHtml(p)}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }

        // By-Hash refs
        if (allPaths.by_hash?.length) {
            const hashRef = details[allPaths.by_hash[0]];
            const fileCount = hashRef?.cache_ids?.length || 1;
            
            html += `
                <div class="refs-section">
                    <div class="refs-title">🔗 By-Hash Refs ${fileCount > 1 ? `<span style="color: var(--color-warning);">(${fileCount} files share this hash)</span>` : ''}</div>
                    <div class="refs-list">
                        ${allPaths.by_hash.map(p => `
                            <div class="ref-item ref-item-link" data-hash-path="${this.escapeHtml(p)}">
                                <span class="ref-item-icon">🔗</span>
                                <span class="ref-item-path">${this.escapeHtml(p)}</span>
                            </div>
                        `).join('')}
                    </div>
                    ${fileCount > 1 ? this.renderSharedHashFiles(hashRef) : ''}
                </div>
            `;
        }

        return html || '<div style="color: var(--color-text-muted);">No refs available</div>';
    }

    renderSharedHashFiles(hashRef) {
        if (!hashRef?.cache_ids?.length) return '';

        return `
            <div class="hash-detail-panel">
                <div class="hash-detail-title">Files sharing this hash:</div>
                <div class="hash-detail-files">
                    ${hashRef.cache_ids.map(item => `
                        <div class="hash-detail-file ${item.cache_id === hashRef.latest_id ? 'latest' : ''}" 
                             data-cache-id="${this.escapeHtml(item.cache_id)}">
                            📄 ${this.escapeHtml(item.cache_id)}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }

    renderDetailError(message) {
        this.detailEmpty.style.display = 'none';
        this.detailContent.style.display = 'flex';
        this.detailContent.innerHTML = `
            <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: var(--color-error);">
                <div style="font-size: 2rem; margin-bottom: 16px;">⚠️</div>
                <div>${this.escapeHtml(message)}</div>
            </div>
        `;
    }

    updateFileItemMeta(cacheId, byId, metadata) {
        const item = this.filesItems.querySelector(`[data-id="${cacheId}"]`);
        if (!item) return;

        const metaEl = item.querySelector('.file-item-meta');
        if (!metaEl) return;

        const strategy = byId.strategy || 'unknown';
        const timestamp = byId.timestamp ? Helpers.formatRelativeTime(byId.timestamp) : '';
        const hash = byId.cache_hash ? byId.cache_hash.substring(0, 8) : '';

        metaEl.innerHTML = `
            <span class="file-item-badge strategy-${strategy}">${strategy}</span>
            ${hash ? `<span title="Hash: ${byId.cache_hash}">🔗 ${hash}...</span>` : ''}
            ${timestamp ? `<span>📅 ${timestamp}</span>` : ''}
        `;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Filtering
    // ═══════════════════════════════════════════════════════════════════════════

    applyFilter() {
        const query = this.filterInput.value.toLowerCase().trim();

        if (!query) {
            this.filteredIds = [...this.fileIds];
        } else {
            this.filteredIds = this.fileIds.filter(id => 
                id.toLowerCase().includes(query)
            );
        }

        this.updateStats();
        this.renderFileList();
        this.focusedIndex = -1;

        if (this.filteredIds.length === 0 && query) {
            this.showState('empty');
        } else {
            this.showState('content');
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Keyboard Navigation
    // ═══════════════════════════════════════════════════════════════════════════

    handleKeydown(e) {
        // Only handle if files view is active
        const activePanel = document.querySelector('.view-panel.active');
        if (!activePanel || activePanel.id !== 'files-view-panel') return;

        // Don't handle if in input
        if (e.target.tagName === 'INPUT') {
            if (e.key === 'Escape') {
                e.target.blur();
            }
            return;
        }

        switch (e.key) {
            case 'ArrowDown':
            case 'j':
                e.preventDefault();
                this.navigateList(1);
                break;
            case 'ArrowUp':
            case 'k':
                e.preventDefault();
                this.navigateList(-1);
                break;
            case 'Enter':
                e.preventDefault();
                if (this.focusedIndex >= 0 && this.focusedIndex < this.filteredIds.length) {
                    this.loadFileDetail(this.filteredIds[this.focusedIndex]);
                }
                break;
            case '/':
                e.preventDefault();
                this.filterInput.focus();
                break;
        }
    }

    navigateList(direction) {
        const items = this.filesItems.querySelectorAll('.file-item');
        if (items.length === 0) return;

        // Remove previous focus
        items.forEach(item => item.classList.remove('focused'));

        // Update index
        this.focusedIndex += direction;
        if (this.focusedIndex < 0) this.focusedIndex = 0;
        if (this.focusedIndex >= items.length) this.focusedIndex = items.length - 1;

        // Apply focus
        const focusedItem = items[this.focusedIndex];
        if (focusedItem) {
            focusedItem.classList.add('focused');
            focusedItem.scrollIntoView({ block: 'nearest' });
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // State Management
    // ═══════════════════════════════════════════════════════════════════════════

    showState(state) {
        this.loadingEl.style.display = state === 'loading' ? 'flex' : 'none';
        this.emptyEl.style.display = state === 'empty' ? 'flex' : 'none';
        this.errorEl.style.display = state === 'error' ? 'flex' : 'none';
        this.filesItems.style.display = state === 'content' ? 'block' : 'none';
    }

    updateStats() {
        const total = this.fileIds.length;
        const filtered = this.filteredIds.length;
        
        if (total === filtered) {
            this.statsEl.textContent = `${total} files`;
        } else {
            this.statsEl.textContent = `${filtered} of ${total} files`;
        }
    }

    updateSelection() {
        this.filesItems.querySelectorAll('.file-item').forEach(item => {
            item.classList.toggle('selected', item.dataset.id === this.selectedId);
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Utilities
    // ═══════════════════════════════════════════════════════════════════════════

    extractKeyPath(byId) {
        const contentPath = byId.file_paths?.content_files?.[0] || '';
        const match = contentPath.match(/key-based\/(.+?)\.json/);
        return match ? match[1] : null;
    }

    getFileIcon(path) {
        if (path.endsWith('.config')) return '⚙️';
        if (path.endsWith('.metadata')) return '🏷️';
        if (path.endsWith('.json')) return '📄';
        return '📁';
    }
}

// Register component
customElements.define('files-view', FilesView);
console.log('✅ FilesView component registered (v0.1.1)');
