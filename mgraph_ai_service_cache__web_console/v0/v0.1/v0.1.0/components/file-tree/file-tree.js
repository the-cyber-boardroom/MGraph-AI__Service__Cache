/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - File Tree Component
   v0.1.0 - Hierarchical file browser for cache entries
   
   Features:
   - Builds tree from flat file paths
   - Expand/collapse folders
   - Filter/search files
   - Keyboard navigation
   - Categorizes by refs/data
   
   Events emitted:
   - file-selected: { path: string, type: string, namespace: string }
   
   Events listened:
   - namespace-changed
   - shortcut:reload
   - shortcut:search-focus
   - shortcut:item-next
   - shortcut:item-prev
   ═══════════════════════════════════════════════════════════════════════════════ */

class FileTree extends BaseComponent {
    constructor() {
        super();
        this.namespace = null;
        this.files = [];
        this.treeData = null;
        this.selectedPath = null;
        this.expandedPaths = new Set();
        this.filterText = '';
        this.visiblePaths = [];
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    bindElements() {
        this.treeRoot = this.$('#tree-root');
        this.loadingState = this.$('#loading-state');
        this.emptyState = this.$('#empty-state');
        this.errorState = this.$('#error-state');
        this.errorMessage = this.$('#error-message');
        this.filterInput = this.$('#filter-input');
        this.fileCount = this.$('#file-count');
        this.refreshBtn = this.$('#refresh-btn');
        this.collapseAllBtn = this.$('#collapse-all-btn');
        this.expandAllBtn = this.$('#expand-all-btn');
        this.retryBtn = this.$('#retry-btn');
        this.statRefs = this.$('#stat-refs');
        this.statData = this.$('#stat-data');
    }

    setupEventListeners() {
        // Filter input
        this.addTrackedListener(this.filterInput, 'input', 
            Helpers.debounce((e) => this.onFilterChange(e.target.value), 150)
        );

        // Buttons
        this.addTrackedListener(this.refreshBtn, 'click', () => this.loadFiles());
        this.addTrackedListener(this.collapseAllBtn, 'click', () => this.collapseAll());
        this.addTrackedListener(this.expandAllBtn, 'click', () => this.expandAll());
        this.addTrackedListener(this.retryBtn, 'click', () => this.loadFiles());

        // Namespace changes
        this.addDocumentListener('namespace-changed', (e) => {
            this.namespace = e.detail.namespace;
            this.loadFiles();
        });

        // Keyboard shortcuts
        this.addDocumentListener('shortcut:reload', () => this.loadFiles());
        this.addDocumentListener('shortcut:search-focus', () => this.filterInput.focus());
        this.addDocumentListener('shortcut:item-next', () => this.selectNextItem());
        this.addDocumentListener('shortcut:item-prev', () => this.selectPrevItem());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Data Loading
    // ═══════════════════════════════════════════════════════════════════════════

    async loadFiles() {
        if (!this.namespace) return;

        this.showState('loading');

        try {
            // Get all files in the namespace
            const result = await cacheApiClient.getAllFiles(this.namespace);
            
            // Handle different response formats
            this.files = Array.isArray(result) ? result : (result.files || []);
            
            if (this.files.length === 0) {
                this.showState('empty');
                return;
            }

            // Build tree structure
            this.treeData = this.buildTree(this.files);
            
            // Update stats
            this.updateStats();
            
            // Render
            this.showState('tree');
            this.render();
            
            // Auto-expand first level
            this.expandFirstLevel();
            
        } catch (error) {
            console.error('Failed to load files:', error);
            this.errorMessage.textContent = error.message || 'Failed to load files';
            this.showState('error');
        }
    }

    buildTree(files) {
        const root = { name: '', children: {}, files: [] };

        for (const filePath of files) {
            const parts = filePath.split('/');
            let current = root;

            for (let i = 0; i < parts.length; i++) {
                const part = parts[i];
                const isFile = i === parts.length - 1;

                if (isFile) {
                    current.files.push({
                        name: part,
                        path: filePath,
                        type: this.getFileType(part)
                    });
                } else {
                    if (!current.children[part]) {
                        current.children[part] = {
                            name: part,
                            path: parts.slice(0, i + 1).join('/'),
                            children: {},
                            files: []
                        };
                    }
                    current = current.children[part];
                }
            }
        }

        return root;
    }

    getFileType(filename) {
        if (filename.endsWith('.json')) return 'json';
        if (filename.endsWith('.config')) return 'config';
        if (filename.endsWith('.metadata')) return 'metadata';
        if (filename.endsWith('.txt')) return 'text';
        if (filename.endsWith('.bin') || filename.endsWith('.binary')) return 'binary';
        return 'file';
    }

    getFileIcon(type) {
        const icons = {
            json: '📄',
            config: '⚙️',
            metadata: '🏷️',
            text: '📝',
            binary: '📦',
            file: '📄',
            folder: '📁'
        };
        return icons[type] || '📄';
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Rendering
    // ═══════════════════════════════════════════════════════════════════════════

    render() {
        if (!this.treeData) return;

        const html = this.renderNode(this.treeData, 0, true);
        this.treeRoot.innerHTML = html;
        this.updateFileCount();
        this.bindTreeEvents();
    }

    renderNode(node, depth, isRoot = false) {
        let html = '';

        // Sort children: folders first, then files
        const folders = Object.values(node.children).sort((a, b) => a.name.localeCompare(b.name));
        const files = node.files.sort((a, b) => a.name.localeCompare(b.name));

        // Render folders
        for (const folder of folders) {
            if (!this.matchesFilter(folder.path)) continue;

            const isExpanded = this.expandedPaths.has(folder.path);
            const isSelected = this.selectedPath === folder.path;
            const hasChildren = Object.keys(folder.children).length > 0 || folder.files.length > 0;

            html += `
                <div class="tree-node" data-path="${this.escapeHtml(folder.path)}">
                    <div class="tree-node-row ${isSelected ? 'selected' : ''}" 
                         data-path="${this.escapeHtml(folder.path)}"
                         data-type="folder"
                         style="padding-left: ${depth * 16 + 8}px">
                        <span class="tree-node-toggle ${hasChildren ? (isExpanded ? 'expanded' : '') : 'hidden'}">▶</span>
                        <span class="tree-node-icon folder">${this.getFileIcon('folder')}</span>
                        <span class="tree-node-name">${this.highlightFilter(folder.name)}</span>
                    </div>
                    <div class="tree-node-children ${isExpanded ? 'expanded' : ''}">
                        ${this.renderNode(folder, depth + 1)}
                    </div>
                </div>
            `;
        }

        // Render files
        for (const file of files) {
            if (!this.matchesFilter(file.path)) continue;

            const isSelected = this.selectedPath === file.path;

            html += `
                <div class="tree-node" data-path="${this.escapeHtml(file.path)}">
                    <div class="tree-node-row ${isSelected ? 'selected' : ''}" 
                         data-path="${this.escapeHtml(file.path)}"
                         data-type="${file.type}"
                         style="padding-left: ${depth * 16 + 8}px">
                        <span class="tree-node-toggle hidden">▶</span>
                        <span class="tree-node-icon file-${file.type}">${this.getFileIcon(file.type)}</span>
                        <span class="tree-node-name ${file.type === 'metadata' || file.type === 'config' ? 'dimmed' : ''}">${this.highlightFilter(file.name)}</span>
                    </div>
                </div>
            `;
        }

        return html;
    }

    bindTreeEvents() {
        // Click on rows
        this.$$('.tree-node-row').forEach(row => {
            this.addTrackedListener(row, 'click', (e) => {
                const path = row.dataset.path;
                const type = row.dataset.type;

                if (type === 'folder') {
                    this.toggleFolder(path);
                } else {
                    this.selectFile(path, type);
                }
            });

            // Double-click to expand/select
            this.addTrackedListener(row, 'dblclick', (e) => {
                const path = row.dataset.path;
                const type = row.dataset.type;

                if (type === 'folder') {
                    // Double-click folder: expand and select first file
                    if (!this.expandedPaths.has(path)) {
                        this.toggleFolder(path);
                    }
                }
            });
        });

        // Build visible paths list for keyboard nav
        this.buildVisiblePaths();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tree Operations
    // ═══════════════════════════════════════════════════════════════════════════

    toggleFolder(path) {
        if (this.expandedPaths.has(path)) {
            this.expandedPaths.delete(path);
        } else {
            this.expandedPaths.add(path);
        }
        this.render();
    }

    expandAll() {
        const addAllPaths = (node, parentPath = '') => {
            for (const [name, child] of Object.entries(node.children)) {
                const path = parentPath ? `${parentPath}/${name}` : name;
                this.expandedPaths.add(child.path || path);
                addAllPaths(child, child.path || path);
            }
        };
        
        if (this.treeData) {
            addAllPaths(this.treeData);
            this.render();
        }
    }

    collapseAll() {
        this.expandedPaths.clear();
        this.render();
    }

    expandFirstLevel() {
        if (this.treeData) {
            for (const child of Object.values(this.treeData.children)) {
                this.expandedPaths.add(child.path);
            }
            this.render();
        }
    }

    selectFile(path, type) {
        // Update selection
        this.selectedPath = path;
        
        // Update UI
        this.$$('.tree-node-row').forEach(row => {
            row.classList.toggle('selected', row.dataset.path === path);
        });

        // Emit event
        this.emit('file-selected', {
            path,
            type,
            namespace: this.namespace
        });

        console.log(`📄 File selected: ${path}`);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Filtering
    // ═══════════════════════════════════════════════════════════════════════════

    onFilterChange(text) {
        this.filterText = text.toLowerCase().trim();
        this.render();
    }

    matchesFilter(path) {
        if (!this.filterText) return true;
        return path.toLowerCase().includes(this.filterText);
    }

    highlightFilter(text) {
        if (!this.filterText) return this.escapeHtml(text);
        
        const escaped = this.escapeHtml(text);
        const regex = new RegExp(`(${this.escapeRegex(this.filterText)})`, 'gi');
        return escaped.replace(regex, '<span class="highlight">$1</span>');
    }

    escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Keyboard Navigation
    // ═══════════════════════════════════════════════════════════════════════════

    buildVisiblePaths() {
        this.visiblePaths = [];
        this.$$('.tree-node-row').forEach(row => {
            // Check if visible (not in collapsed parent)
            const node = row.closest('.tree-node');
            const parent = node?.parentElement?.closest('.tree-node-children');
            
            if (!parent || parent.classList.contains('expanded')) {
                this.visiblePaths.push(row.dataset.path);
            }
        });
    }

    selectNextItem() {
        if (this.visiblePaths.length === 0) return;
        
        const currentIndex = this.selectedPath ? this.visiblePaths.indexOf(this.selectedPath) : -1;
        const nextIndex = Math.min(currentIndex + 1, this.visiblePaths.length - 1);
        
        const nextPath = this.visiblePaths[nextIndex];
        const row = this.$(`.tree-node-row[data-path="${nextPath}"]`);
        
        if (row) {
            this.selectFile(nextPath, row.dataset.type);
            row.scrollIntoView({ block: 'nearest' });
        }
    }

    selectPrevItem() {
        if (this.visiblePaths.length === 0) return;
        
        const currentIndex = this.selectedPath ? this.visiblePaths.indexOf(this.selectedPath) : this.visiblePaths.length;
        const prevIndex = Math.max(currentIndex - 1, 0);
        
        const prevPath = this.visiblePaths[prevIndex];
        const row = this.$(`.tree-node-row[data-path="${prevPath}"]`);
        
        if (row) {
            this.selectFile(prevPath, row.dataset.type);
            row.scrollIntoView({ block: 'nearest' });
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Stats & State
    // ═══════════════════════════════════════════════════════════════════════════

    updateStats() {
        let refsCount = 0;
        let dataCount = 0;

        for (const file of this.files) {
            if (file.includes('/refs/')) {
                refsCount++;
            } else if (file.includes('/data/')) {
                dataCount++;
            }
        }

        this.statRefs.textContent = `refs: ${refsCount}`;
        this.statData.textContent = `data: ${dataCount}`;
    }

    updateFileCount() {
        const visibleCount = this.$$('.tree-node-row[data-type]:not([data-type="folder"])').length;
        this.fileCount.textContent = `${visibleCount} files`;
    }

    showState(state) {
        this.loadingState.style.display = state === 'loading' ? 'flex' : 'none';
        this.emptyState.style.display = state === 'empty' ? 'flex' : 'none';
        this.errorState.style.display = state === 'error' ? 'flex' : 'none';
        this.treeRoot.style.display = state === 'tree' ? 'block' : 'none';
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    setNamespace(namespace) {
        this.namespace = namespace;
        this.loadFiles();
    }

    getSelectedPath() {
        return this.selectedPath;
    }

    refresh() {
        this.loadFiles();
    }
}

// Register component
customElements.define('file-tree', FileTree);
console.log('✅ FileTree component registered');
