/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Main Orchestrator
   v0.1.0 - Coordinates all components and manages application state
   
   This is the main entry point that:
   - Initializes all components
   - Sets up global event listeners
   - Coordinates inter-component communication
   - Manages application state
   ═══════════════════════════════════════════════════════════════════════════════ */

class CacheBrowser {
    constructor() {
        this.version = '0.1.0';
        this.currentNamespace = null;
        this.isInitialized = false;
        
        // Component references
        this.topNav = null;
        this.fileTree = null;
        this.contentViewer = null;
        this.keyboardShortcuts = null;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Initialization
    // ═══════════════════════════════════════════════════════════════════════════

    async init() {
        console.log(`🗄️ Cache Browser v${this.version} initializing...`);

        try {
            // Initialize keyboard shortcuts first (before components)
            this.initKeyboardShortcuts();

            // Wait for all components to be ready
            await this.waitForComponents();

            // Get component references
            this.topNav = document.querySelector('top-nav');
            this.fileTree = document.querySelector('file-tree');
            this.contentViewer = document.querySelector('content-viewer');

            // Setup event coordination
            this.setupEventCoordination();

            // Mark as initialized
            this.isInitialized = true;

            console.log(`✅ Cache Browser v${this.version} ready`);

        } catch (error) {
            console.error('Failed to initialize Cache Browser:', error);
            this.showFatalError(error.message);
        }
    }

    initKeyboardShortcuts() {
        // Initialize keyboard shortcuts manager
        const configPath = ComponentPaths.getDataPath('../../v0.1.0/data/keyboard-shortcuts.json');
        this.keyboardShortcuts = new KeyboardShortcuts({
            configUrl: configPath
        });

        console.log('⌨️ Keyboard shortcuts initialized');
    }

    async waitForComponents() {
        const components = [
            'top-nav',
            'file-tree',
            'content-viewer'
        ];

        const promises = components.map(tagName => {
            return customElements.whenDefined(tagName);
        });

        await Promise.all(promises);

        // Also wait for each component to emit ready
        const readyPromises = components.map(tagName => {
            const el = document.querySelector(tagName);
            if (el && el.whenReady) {
                return el.whenReady(10000);
            }
            return Promise.resolve();
        });

        await Promise.all(readyPromises);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Event Coordination
    // ═══════════════════════════════════════════════════════════════════════════

    setupEventCoordination() {
        // Namespace changes propagate from top-nav to file-tree
        document.addEventListener('namespace-changed', (e) => {
            this.currentNamespace = e.detail.namespace;
            console.log(`📂 Namespace changed to: ${this.currentNamespace}`);
            
            // Clear content viewer when namespace changes
            if (this.contentViewer) {
                this.contentViewer.clear();
            }
        });

        // File selection propagates from file-tree to content-viewer
        document.addEventListener('file-selected', (e) => {
            console.log(`📄 File selected: ${e.detail.path}`);
        });

        // Content loaded event
        document.addEventListener('content-loaded', (e) => {
            console.log(`📥 Content loaded: ${e.detail.path} (${e.detail.type}, ${Helpers.formatBytes(e.detail.size)})`);
        });

        // Auth error handling
        document.addEventListener('cache-api:auth-error', (e) => {
            console.warn('🔒 Auth error:', e.detail);
            // TopNav handles showing the auth banner
        });

        // Network error handling
        document.addEventListener('cache-api:network-error', (e) => {
            console.error('🌐 Network error:', e.detail);
        });

        // Component ready events (for debugging)
        document.addEventListener('component-ready', (e) => {
            console.log(`📦 Component ready: ${e.detail.component}`);
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Error Handling
    // ═══════════════════════════════════════════════════════════════════════════

    showFatalError(message) {
        const main = document.querySelector('.app-main');
        if (main) {
            main.innerHTML = `
                <div class="fatal-error">
                    <div class="empty-state-icon">💥</div>
                    <h2>Failed to Initialize</h2>
                    <p>${Helpers.escapeHtml(message)}</p>
                    <button class="btn btn-primary" onclick="location.reload()">Reload Page</button>
                </div>
            `;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    getVersion() {
        return this.version;
    }

    getCurrentNamespace() {
        return this.currentNamespace;
    }

    refresh() {
        if (this.topNav) {
            this.topNav.refresh();
        }
        if (this.fileTree) {
            this.fileTree.refresh();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Application Entry Point
// ═══════════════════════════════════════════════════════════════════════════════

// Create global instance
window.cacheBrowser = new CacheBrowser();

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.cacheBrowser.init();
});

console.log('✅ CacheBrowser orchestrator loaded');
