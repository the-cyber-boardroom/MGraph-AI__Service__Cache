/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Hashes View Component
   v0.1.1 - List cache entries by content hash
   
   Events emitted:
   - hash-detail-loaded: { hash, refs }
   - navigate-to-file: { cacheId }
   
   Events listened:
   - namespace-changed
   - view-changed
   ═══════════════════════════════════════════════════════════════════════════════ */

class HashesView extends BaseComponent {
    constructor() {
        super();
        this.namespace = null;
        this.hashes = [];
        this.filteredHashes = [];
        this.selectedHash = null;
        this.focusedIndex = -1;
        this.hashDetails = new Map(); // Cache loaded hash details
    }

    bindElements() {
        this.filterInput = this.$('#hashes-filter');
        this.statsEl = this.$('#hashes-stats');
        this.hashesList = this.$('#hashes-list');
        this.hashesItems = this.$('#hashes-items');
        this.loadingEl = this.$('#hashes-loading');
        this.emptyEl = this.$('#hashes-empty');
        this.errorEl = this.$('#hashes-error');
        this.errorMessage = this.$('#hashes-error-message');
        this.retryBtn = this.$('#hashes-retry-btn');
        this.detailEmpty = this.$('#hash-detail-empty');
        this.detailContent = this.$('#hash-detail-content');
    }

    setupEventListeners() {
        // Filter input
        this.addTrackedListener(this.filterInput, 'input', 
            Helpers.debounce(() => this.applyFilter(), 150)
        );

        // Retry button
        this.addTrackedListener(this.retryBtn, 'click', () => this.loadHashes());

        // Namespace changes
        this.addDocumentListener('namespace-changed', (e) => {
            this.namespace = e.detail.namespace;
            this.hashDetails.clear();
            this.loadHashes();
        });

        // View changes - load data when switching to hashes view
        this.addDocumentListener('view-changed', (e) => {
            if (e.detail.view === 'hashes' && this.namespace && this.hashes.length === 0) {
                this.loadHashes();
            }
        });

        // Keyboard navigation
        this.addDocumentListener('keydown', (e) => this.handleKeydown(e));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Data Loading
    // ═══════════════════════════════════════════════════════════════════════════

    async loadHashes() {
        if (!this.namespace) return;

        this.showState('loading');

        try {
            this.hashes = await cacheApiClient.getFileHashes(this.namespace);
            this.filteredHashes = [...this.hashes];
            this.updateStats();
            this.renderHashList();
            
            if (this.hashes.length === 0) {
                this.showState('empty');
            } else {
                this.showState('content');
            }
        } catch (error) {
            console.error('Failed to load hashes:', error);
            this.errorMessage.textContent = error.message || 'Failed to load hashes';
            this.showState('error');
        }
    }

    async loadHashDetail(hash) {
        if (!this.namespace || !hash) return;

        this.selectedHash = hash;
        this.updateSelection();

        // Check cache first
        if (this.hashDetails.has(hash)) {
            this.renderDetail(hash, this.hashDetails.get(hash));
            return;
        }

        try {
            // Load hash refs
            const refs = await cacheApiClient.retrieveHashRefs(this.namespace, hash);
            this.hashDetails.set(hash, refs);
            this.renderDetail(hash, refs);
            
            this.emit('hash-detail-loaded', { hash, refs });
        } catch (error) {
            console.error('Failed to load hash detail:', error);
            this.renderDetailError(error.message);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Rendering
    // ═══════════════════════════════════════════════════════════════════════════

    renderHashList() {
        this.hashesItems.innerHTML = '';

        this.filteredHashes.forEach((hash, index) => {
            const item = document.createElement('div');
            item.className = 'hash-item';
            item.dataset.hash = hash;
            item.dataset.index = index;
            
            item.innerHTML = `
                <span class="hash-item-icon">🔗</span>
                <div class="hash-item-content">
                    <div class="hash-item-value">${this.escapeHtml(hash)}</div>
                    <div class="hash-item-meta">
                        <span class="hash-item-count">Click to load file count</span>
                    </div>
                </div>
            `;

            item.addEventListener('click', () => {
                this.focusedIndex = index;
                this.loadHashDetail(hash);
            });

            this.hashesItems.appendChild(item);
        });
    }

    renderDetail(hash, refs) {
        this.detailEmpty.style.display = 'none';
        this.detailContent.style.display = 'block';

        const cacheIds = refs.cache_ids || [];
        const latestId = refs.latest_id;
        const totalVersions = refs.total_versions || cacheIds.length;
        const hasDuplicates = cacheIds.length > 1;

        this.detailContent.innerHTML = `
            <div class="hash-detail-header">
                <span class="hash-item-icon" style="font-size: 1.5rem;">🔗</span>
                <div class="hash-detail-hash">${this.escapeHtml(hash)}</div>
                <button class="btn btn-ghost btn-sm" id="hash-copy-btn" title="Copy hash">📋</button>
            </div>
            
            <div class="hash-detail-body">
                ${hasDuplicates ? `
                    <div class="duplicates-warning">
                        <span class="duplicates-warning-icon">⚠️</span>
                        <span><strong>${cacheIds.length} files</strong> share this content hash (potential duplicates)</span>
                    </div>
                ` : ''}
                
                <div class="hash-detail-section">
                    <div class="hash-detail-section-title">
                        📄 Files with this hash (${cacheIds.length})
                    </div>
                    <div class="hash-file-list">
                        ${cacheIds.map(item => `
                            <div class="hash-file-item ${item.cache_id === latestId ? 'is-latest' : ''}" 
                                 data-cache-id="${this.escapeHtml(item.cache_id)}">
                                <span style="font-size: 1rem;">📄</span>
                                <span class="hash-file-item-id">${this.escapeHtml(item.cache_id)}</span>
                                ${item.timestamp ? `
                                    <span class="hash-file-item-meta">${Helpers.formatRelativeTime(item.timestamp)}</span>
                                ` : ''}
                                ${item.cache_id === latestId ? `
                                    <span class="hash-file-item-badge">latest</span>
                                ` : ''}
                            </div>
                        `).join('')}
                    </div>
                </div>
                
                <div class="hash-detail-section">
                    <div class="hash-detail-section-title">ℹ️ Hash Info</div>
                    <div class="info-grid">
                        <span class="info-label">Total Versions:</span>
                        <span class="info-value">${totalVersions}</span>
                        
                        <span class="info-label">Latest ID:</span>
                        <span class="info-value highlight">${this.escapeHtml(latestId || 'N/A')}</span>
                        
                        <span class="info-label">Status:</span>
                        <span class="info-value">${hasDuplicates ? '⚠️ Duplicates detected' : '✅ Unique'}</span>
                    </div>
                </div>
            </div>
        `;

        // Bind copy button
        this.detailContent.querySelector('#hash-copy-btn')?.addEventListener('click', () => {
            Helpers.copyToClipboard(hash);
        });

        // Bind file item clicks - navigate to files view
        this.detailContent.querySelectorAll('.hash-file-item').forEach(item => {
            item.addEventListener('click', () => {
                const cacheId = item.dataset.cacheId;
                this.navigateToFile(cacheId);
            });
        });

        // Update the list item with file count
        this.updateHashItemMeta(hash, cacheIds.length);
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

    updateHashItemMeta(hash, fileCount) {
        const item = this.hashesItems.querySelector(`[data-hash="${hash}"]`);
        if (!item) return;

        const metaEl = item.querySelector('.hash-item-meta');
        if (!metaEl) return;

        const hasDuplicates = fileCount > 1;
        metaEl.innerHTML = `
            <span class="hash-item-count ${hasDuplicates ? 'multiple' : ''}">
                📄 ${fileCount} file${fileCount !== 1 ? 's' : ''}
                ${hasDuplicates ? ' ⚠️' : ''}
            </span>
        `;

        // Add duplicate class to item
        item.classList.toggle('has-duplicates', hasDuplicates);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Navigation
    // ═══════════════════════════════════════════════════════════════════════════

    navigateToFile(cacheId) {
        // Switch to files view and select this file
        this.emit('navigate-to-file', { cacheId });
        
        // Get the files view component and trigger selection
        const filesView = document.querySelector('files-view');
        if (filesView) {
            // Switch view first
            const viewTabs = document.querySelector('view-tabs');
            if (viewTabs) {
                viewTabs.switchView('files');
            }
            
            // Then load the file detail
            setTimeout(() => {
                filesView.loadFileDetail(cacheId);
            }, 100);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Filtering
    // ═══════════════════════════════════════════════════════════════════════════

    applyFilter() {
        const query = this.filterInput.value.toLowerCase().trim();

        if (!query) {
            this.filteredHashes = [...this.hashes];
        } else {
            this.filteredHashes = this.hashes.filter(hash => 
                hash.toLowerCase().includes(query)
            );
        }

        this.updateStats();
        this.renderHashList();
        this.focusedIndex = -1;

        if (this.filteredHashes.length === 0 && query) {
            this.showState('empty');
        } else {
            this.showState('content');
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Keyboard Navigation
    // ═══════════════════════════════════════════════════════════════════════════

    handleKeydown(e) {
        // Only handle if hashes view is active
        const activePanel = document.querySelector('.view-panel.active');
        if (!activePanel || activePanel.id !== 'hashes-view-panel') return;

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
                if (this.focusedIndex >= 0 && this.focusedIndex < this.filteredHashes.length) {
                    this.loadHashDetail(this.filteredHashes[this.focusedIndex]);
                }
                break;
            case '/':
                e.preventDefault();
                this.filterInput.focus();
                break;
        }
    }

    navigateList(direction) {
        const items = this.hashesItems.querySelectorAll('.hash-item');
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
        this.hashesItems.style.display = state === 'content' ? 'block' : 'none';
    }

    updateStats() {
        const total = this.hashes.length;
        const filtered = this.filteredHashes.length;
        
        if (total === filtered) {
            this.statsEl.textContent = `${total} hashes`;
        } else {
            this.statsEl.textContent = `${filtered} of ${total} hashes`;
        }
    }

    updateSelection() {
        this.hashesItems.querySelectorAll('.hash-item').forEach(item => {
            item.classList.toggle('selected', item.dataset.hash === this.selectedHash);
        });
    }
}

// Register component
customElements.define('hashes-view', HashesView);
console.log('✅ HashesView component registered (v0.1.1)');
