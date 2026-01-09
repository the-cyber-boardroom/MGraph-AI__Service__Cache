/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Component Paths Resolver
   v0.1.0 - Resolves paths for component resources across versions
   
   This enables IFD version independence:
   - v0.1.1 can reference v0.1.0 components
   - Each version only contains new/changed files
   - Paths are resolved dynamically based on version
   ═══════════════════════════════════════════════════════════════════════════════ */

const ComponentPaths = {
    // Current version - set by index.html
    version: 'v0.1.0',
    
    // Base path to the v0.1 folder
    basePath: '',
    
    // Shared CSS path
    sharedCss: '',
    
    // Component registry: maps component names to their version
    // When a component is added/changed in a version, register it here
    registry: {
        // v0.1.0 components
        'top-nav': 'v0.1.0',
        'namespace-tabs': 'v0.1.0',
        'status-bar': 'v0.1.0',
        'file-tree': 'v0.1.0',
        'content-viewer': 'v0.1.0',
        
        // v0.1.1 would add:
        // 'cache-list': 'v0.1.1',
        // 'cache-item': 'v0.1.1',
        
        // v0.1.2 would add:
        // 'cache-detail': 'v0.1.2',
    },

    /**
     * Initialize paths based on current document location
     * Called once from index.html
     */
    init(currentVersion = 'v0.1.0') {
        this.version = currentVersion;
        
        // Detect base path from script location
        const scripts = document.getElementsByTagName('script');
        for (const script of scripts) {
            if (script.src && script.src.includes('component-paths.js')) {
                // Extract base path: .../v0.1/v0.1.0/js/utils/component-paths.js
                const parts = script.src.split('/');
                const utilsIndex = parts.indexOf('utils');
                if (utilsIndex > 0) {
                    // Go back to v0.1 folder
                    this.basePath = parts.slice(0, utilsIndex - 2).join('/');
                }
                break;
            }
        }
        
        // If detection failed, use relative path
        if (!this.basePath) {
            this.basePath = '..';
        }
        
        // Set shared CSS path
        this.sharedCss = `${this.basePath}/${this.version}/css/common.css`;
        
        console.log(`✅ ComponentPaths initialized: version=${this.version}, basePath=${this.basePath}`);
    },

    /**
     * Get paths for a component
     * @param {string} componentName - Component tag name (e.g., 'top-nav')
     * @returns {Object} { js, html, css } paths
     */
    getComponentPaths(componentName) {
        // Look up which version has this component
        const componentVersion = this.registry[componentName] || this.version;
        
        const componentBase = `${this.basePath}/${componentVersion}/components/${componentName}`;
        
        return {
            js: `${componentBase}/${componentName}.js`,
            html: `${componentBase}/${componentName}.html`,
            css: `${componentBase}/${componentName}.css`
        };
    },

    /**
     * Get path to a service/utility
     * @param {string} servicePath - Relative path from js/ folder
     * @param {string} version - Specific version (optional, defaults to current)
     * @returns {string} Full path
     */
    getServicePath(servicePath, version = null) {
        const v = version || this.version;
        return `${this.basePath}/${v}/js/${servicePath}`;
    },

    /**
     * Get path to data file
     * @param {string} dataPath - Relative path from data/ folder
     * @param {string} version - Specific version (optional)
     * @returns {string} Full path
     */
    getDataPath(dataPath, version = null) {
        const v = version || this.version;
        return `${this.basePath}/${v}/data/${dataPath}`;
    },

    /**
     * Register a component for a specific version
     * Used by newer versions to override or add components
     * @param {string} componentName - Component name
     * @param {string} version - Version string
     */
    registerComponent(componentName, version) {
        this.registry[componentName] = version;
        console.log(`📦 Registered component: ${componentName} -> ${version}`);
    },

    /**
     * Bulk register multiple components
     * @param {Object} components - Map of componentName -> version
     */
    registerComponents(components) {
        for (const [name, version] of Object.entries(components)) {
            this.registry[name] = version;
        }
        console.log(`📦 Registered ${Object.keys(components).length} components`);
    },

    /**
     * Get the version that provides a component
     * @param {string} componentName - Component name
     * @returns {string} Version string
     */
    getComponentVersion(componentName) {
        return this.registry[componentName] || this.version;
    },

    /**
     * Check if a component is registered
     * @param {string} componentName - Component name
     * @returns {boolean}
     */
    hasComponent(componentName) {
        return componentName in this.registry;
    },

    /**
     * List all registered components
     * @returns {Object} Component registry
     */
    listComponents() {
        return { ...this.registry };
    },

    /**
     * Group components by version
     * @returns {Object} Grouped components
     */
    groupByVersion() {
        const groups = {};
        for (const [name, version] of Object.entries(this.registry)) {
            if (!groups[version]) {
                groups[version] = [];
            }
            groups[version].push(name);
        }
        return groups;
    }
};

// Export for both browser and Node.js
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ComponentPaths;
}
if (typeof window !== 'undefined') {
    window.ComponentPaths = ComponentPaths;
}

console.log('✅ ComponentPaths loaded');
