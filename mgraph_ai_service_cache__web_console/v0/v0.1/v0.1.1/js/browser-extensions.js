/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.1 Orchestrator Extensions
   
   Extends the v0.1.0 CacheBrowser class with v0.1.1 features:
   - View management (Files, Hashes, Raw)
   - Cross-view navigation
   ═══════════════════════════════════════════════════════════════════════════════ */

(function() {
    // Wait for base CacheBrowser to be available
    const checkBase = setInterval(() => {
        if (window.cacheBrowser && window.cacheBrowser.isInitialized) {
            clearInterval(checkBase);
            extendCacheBrowser();
        }
    }, 100);

    // Give up after 10 seconds
    setTimeout(() => clearInterval(checkBase), 10000);

    function extendCacheBrowser() {
        const browser = window.cacheBrowser;
        
        // Store original version
        browser.baseVersion = browser.version;
        browser.version = '0.1.1';

        // Get view components
        browser.viewTabs = document.querySelector('view-tabs');
        browser.filesView = document.querySelector('files-view');
        browser.hashesView = document.querySelector('hashes-view');

        // Setup cross-view navigation
        setupCrossViewNavigation();

        // Log extension
        console.log(`🔄 CacheBrowser extended from v${browser.baseVersion} to v${browser.version}`);
    }

    function setupCrossViewNavigation() {
        // Listen for navigate-to-file events from hashes view
        document.addEventListener('navigate-to-file', (e) => {
            console.log(`🔀 Navigating to file: ${e.detail.cacheId}`);
        });

        // Listen for view changes to sync sidebar visibility
        document.addEventListener('view-changed', (e) => {
            const sidebar = document.getElementById('sidebar');
            const resizeHandle = document.getElementById('sidebar-resize-handle');
            
            if (e.detail.view === 'raw') {
                sidebar.style.display = 'flex';
                resizeHandle.style.display = 'block';
                // Update handle position after sidebar is shown
                setTimeout(() => {
                    if (window.sidebarResizer) {
                        window.sidebarResizer.updateHandlePosition();
                    }
                }, 10);
            } else {
                sidebar.style.display = 'none';
                resizeHandle.style.display = 'none';
            }
        });
    }
})();
