/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Top Navigation Component
   v0.1.0 - Header with namespace tabs, status indicator, and help button
   
   Events emitted:
   - namespace-changed: { namespace: string, index: number }
   
   Events listened:
   - shortcut:namespace-next
   - shortcut:namespace-prev
   - shortcut:namespace-select
   - shortcut:help-toggle
   - cache-api:auth-error
   ═══════════════════════════════════════════════════════════════════════════════ */

class TopNav extends BaseComponent {
    constructor() {
        super();
        this.namespaces = [];
        this.activeNamespace = null;
        this.activeIndex = 0;
        this.isHealthy = false;
        this.storageMode = null;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    bindElements() {
        this.namespaceTabs = this.$('#namespace-tabs');
        this.healthDot = this.$('#health-dot');
        this.healthText = this.$('#health-text');
        this.storageBadge = this.$('#storage-badge');
        this.storageModeEl = this.$('#storage-mode');
        this.helpBtn = this.$('#help-btn');
        this.authBanner = this.$('#auth-banner');
        this.authRetryBtn = this.$('#auth-retry-btn');
    }

    setupEventListeners() {
        // Help button
        this.addTrackedListener(this.helpBtn, 'click', () => {
            document.dispatchEvent(new CustomEvent('shortcut:help-toggle'));
        });

        // Auth retry
        this.addTrackedListener(this.authRetryBtn, 'click', () => {
            this.hideAuthBanner();
            this.checkHealth();
            this.loadNamespaces();
        });

        // Keyboard shortcuts
        this.addDocumentListener('shortcut:namespace-next', () => this.selectNextNamespace());
        this.addDocumentListener('shortcut:namespace-prev', () => this.selectPrevNamespace());
        this.addDocumentListener('shortcut:namespace-select', (e) => {
            const index = e.detail?.index;
            if (typeof index === 'number') {
                this.selectNamespaceByIndex(index);
            }
        });

        // Auth errors
        this.addDocumentListener('cache-api:auth-error', () => {
            this.showAuthBanner();
        });
    }

    onReady() {
        this.checkHealth();
        this.loadStorageInfo();
        this.loadNamespaces();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // API Calls
    // ═══════════════════════════════════════════════════════════════════════════

    async checkHealth() {
        this.setHealthState('checking');
        
        try {
            const result = await cacheApiClient.getHealth();
            this.isHealthy = result?.status === 'ok';
            this.setHealthState(this.isHealthy ? 'online' : 'offline');
        } catch (error) {
            console.error('Health check failed:', error);
            this.isHealthy = false;
            this.setHealthState('offline');
        }
    }

    async loadStorageInfo() {
        try {
            const info = await cacheApiClient.getStorageInfo();
            this.storageMode = info?.storage_mode || info?.mode;
            
            if (this.storageMode) {
                this.storageModeEl.textContent = this.storageMode.toUpperCase();
                this.storageBadge.style.display = 'flex';
            }
        } catch (error) {
            console.warn('Failed to load storage info:', error);
        }
    }

    async loadNamespaces() {
        try {
            const namespaces = await cacheApiClient.getNamespaces();
            this.namespaces = Array.isArray(namespaces) ? namespaces : [];
            
            // Sort namespaces (put 'default' first if present)
            this.namespaces.sort((a, b) => {
                if (a === 'default') return -1;
                if (b === 'default') return 1;
                return a.localeCompare(b);
            });
            
            this.renderNamespaceTabs();
            
            // Select first namespace
            if (this.namespaces.length > 0) {
                this.selectNamespace(this.namespaces[0], 0);
            }
        } catch (error) {
            console.error('Failed to load namespaces:', error);
            this.namespaceTabs.innerHTML = `
                <div class="namespace-loading">
                    <span>❌ Failed to load namespaces</span>
                </div>
            `;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UI Updates
    // ═══════════════════════════════════════════════════════════════════════════

    setHealthState(state) {
        this.healthDot.className = 'status-dot ' + state;
        
        switch (state) {
            case 'online':
                this.healthText.textContent = 'Healthy';
                break;
            case 'offline':
                this.healthText.textContent = 'Offline';
                break;
            case 'checking':
                this.healthText.textContent = 'Checking...';
                break;
        }
    }

    renderNamespaceTabs() {
        if (this.namespaces.length === 0) {
            this.namespaceTabs.innerHTML = `
                <div class="namespace-loading">
                    <span>No namespaces found</span>
                </div>
            `;
            return;
        }

        const html = this.namespaces.map((ns, index) => {
            const isActive = ns === this.activeNamespace;
            const shortcutHint = index < 5 ? ` (${index + 1})` : '';
            return `
                <button class="namespace-tab ${isActive ? 'active' : ''}" 
                        data-namespace="${this.escapeHtml(ns)}"
                        data-index="${index}"
                        title="${this.escapeHtml(ns)}${shortcutHint}">
                    ${this.escapeHtml(ns)}
                </button>
            `;
        }).join('');

        this.namespaceTabs.innerHTML = html;

        // Add click handlers
        this.$$('.namespace-tab').forEach(tab => {
            this.addTrackedListener(tab, 'click', () => {
                const ns = tab.dataset.namespace;
                const idx = parseInt(tab.dataset.index, 10);
                this.selectNamespace(ns, idx);
            });
        });
    }

    selectNamespace(namespace, index) {
        if (this.activeNamespace === namespace) return;
        
        this.activeNamespace = namespace;
        this.activeIndex = index;
        
        // Update tab states
        this.$$('.namespace-tab').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.namespace === namespace);
        });

        // Emit event
        this.emit('namespace-changed', { 
            namespace, 
            index 
        });

        console.log(`📂 Namespace selected: ${namespace}`);
    }

    selectNextNamespace() {
        if (this.namespaces.length === 0) return;
        const nextIndex = (this.activeIndex + 1) % this.namespaces.length;
        this.selectNamespace(this.namespaces[nextIndex], nextIndex);
    }

    selectPrevNamespace() {
        if (this.namespaces.length === 0) return;
        const prevIndex = (this.activeIndex - 1 + this.namespaces.length) % this.namespaces.length;
        this.selectNamespace(this.namespaces[prevIndex], prevIndex);
    }

    selectNamespaceByIndex(index) {
        if (index >= 0 && index < this.namespaces.length) {
            this.selectNamespace(this.namespaces[index], index);
        }
    }

    showAuthBanner() {
        this.authBanner.classList.add('show');
    }

    hideAuthBanner() {
        this.authBanner.classList.remove('show');
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    getActiveNamespace() {
        return this.activeNamespace;
    }

    getNamespaces() {
        return [...this.namespaces];
    }

    async refresh() {
        await this.checkHealth();
        await this.loadNamespaces();
    }
}

// Register component
customElements.define('top-nav', TopNav);
console.log('✅ TopNav component registered');
