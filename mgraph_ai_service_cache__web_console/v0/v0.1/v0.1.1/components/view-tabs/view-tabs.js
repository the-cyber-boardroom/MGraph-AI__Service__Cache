/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - View Tabs Component
   v0.1.1 - Tab switcher for Files/Hashes/Raw views
   
   Events emitted:
   - view-changed: { view: 'files' | 'hashes' | 'raw', namespace }
   
   Events listened:
   - namespace-changed
   ═══════════════════════════════════════════════════════════════════════════════ */

class ViewTabs extends BaseComponent {
    constructor() {
        super();
        this.activeView = 'raw';
        this.currentNamespace = null;
        this.fileCounts = { files: null, hashes: null };
    }

    bindElements() {
        this.tabButtons = this.$$('.view-tab-btn');
        this.filesCount = this.$('#files-count');
        this.hashesCount = this.$('#hashes-count');
        this.namespaceDisplay = this.$('#current-namespace');
    }

    setupEventListeners() {
        // Tab button clicks
        this.tabButtons.forEach(btn => {
            this.addTrackedListener(btn, 'click', () => {
                this.switchView(btn.dataset.view);
            });
        });

        // Listen for namespace changes
        this.addDocumentListener('namespace-changed', (e) => {
            this.currentNamespace = e.detail.namespace;
            this.namespaceDisplay.textContent = this.currentNamespace;
            this.loadCounts();
        });

        // Keyboard shortcuts for view switching
        this.addDocumentListener('shortcut:view-files', () => this.switchView('files'));
        this.addDocumentListener('shortcut:view-hashes', () => this.switchView('hashes'));
        this.addDocumentListener('shortcut:view-raw', () => this.switchView('raw'));
    }

    switchView(view) {
        if (this.activeView === view) return;
        
        this.activeView = view;

        // Update tab buttons
        this.tabButtons.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.view === view);
        });

        // Update view panels
        document.querySelectorAll('.view-panel').forEach(panel => {
            panel.classList.remove('active');
            panel.style.display = 'none';
        });

        const activePanel = document.getElementById(`${view}-view-panel`);
        if (activePanel) {
            activePanel.classList.add('active');
            activePanel.style.display = 'flex';
        }

        // Show/hide sidebar based on view
        const sidebar = document.getElementById('sidebar');
        const resizeHandle = document.getElementById('sidebar-resize-handle');
        
        if (view === 'raw') {
            sidebar.style.display = 'flex';
            resizeHandle.style.display = 'block';
        } else {
            sidebar.style.display = 'none';
            resizeHandle.style.display = 'none';
        }

        // Emit view change event
        this.emit('view-changed', { 
            view, 
            namespace: this.currentNamespace 
        });

        console.log(`📊 View changed to: ${view}`);
    }

    async loadCounts() {
        if (!this.currentNamespace) return;

        // Load file IDs count
        try {
            const fileIds = await cacheApiClient.getFileIds(this.currentNamespace);
            this.fileCounts.files = Array.isArray(fileIds) ? fileIds.length : 0;
            this.filesCount.textContent = this.formatCount(this.fileCounts.files);
        } catch (error) {
            console.error('Failed to load file IDs:', error);
            this.filesCount.textContent = '?';
        }

        // Load hashes count
        try {
            const hashes = await cacheApiClient.getFileHashes(this.currentNamespace);
            this.fileCounts.hashes = Array.isArray(hashes) ? hashes.length : 0;
            this.hashesCount.textContent = this.formatCount(this.fileCounts.hashes);
        } catch (error) {
            console.error('Failed to load file hashes:', error);
            this.hashesCount.textContent = '?';
        }
    }

    formatCount(count) {
        if (count === null) return '-';
        if (count >= 1000) return `${(count / 1000).toFixed(1)}k`;
        return count.toString();
    }

    // Public API
    getActiveView() {
        return this.activeView;
    }

    getCounts() {
        return this.fileCounts;
    }
}

// Register component
customElements.define('view-tabs', ViewTabs);
console.log('✅ ViewTabs component registered (v0.1.1)');
