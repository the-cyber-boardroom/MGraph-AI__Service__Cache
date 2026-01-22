/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.3 Orchestrator Extensions
   
   - Integrates HTML detection with 3rd column panel
   - Updates Open in Browser link
   - Removes the inline HTML tab (replaced by panel)
   ═══════════════════════════════════════════════════════════════════════════════ */

(function() {
    // Configuration
    const API_BASE_URL = '';
    
    // Wait for content-viewer to be ready
    const checkReady = setInterval(() => {
        const contentViewer = document.querySelector('content-viewer');
        if (contentViewer && contentViewer.shadowRoot && window.cacheBrowser?.isInitialized) {
            clearInterval(checkReady);
            extendContentViewer(contentViewer);
        }
    }, 100);

    setTimeout(() => clearInterval(checkReady), 10000);

    function extendContentViewer(contentViewer) {
        const shadowRoot = contentViewer.shadowRoot;
        
        // Store original loadFile method
        const originalLoadFile = contentViewer.loadFile?.bind(contentViewer);
        
        if (!originalLoadFile) {
            console.warn('content-viewer.loadFile not found');
            return;
        }

        // Add "Open in Browser" button (replaces v0.1.2)
        addOpenInBrowserButton(shadowRoot);

        // Override loadFile to detect HTML and show in panel
        contentViewer.loadFile = async function(pathOrDetail, options = {}) {
            // Extract path string
            let pathString;
            if (typeof pathOrDetail === 'string') {
                pathString = pathOrDetail;
            } else if (pathOrDetail && typeof pathOrDetail === 'object') {
                pathString = pathOrDetail.path || pathOrDetail.filePath || pathOrDetail.name;
            }
            
            if (!pathString) {
                console.warn('loadFile: Could not extract path from:', pathOrDetail);
                pathString = '';
            }
            
            // Store the path
            contentViewer._currentPath = pathString;
            
            console.log('📂 Loading file:', pathString);
            
            // Call original
            await originalLoadFile(pathOrDetail, options);
            
            // Update Open in Browser link
            updateOpenInBrowserLink(shadowRoot, pathString);
            
            // Check for HTML content and show in panel
            setTimeout(() => {
                detectAndShowHtml(contentViewer, pathString);
            }, 300);
        };

        // Update version
        if (window.cacheBrowser) {
            window.cacheBrowser.version = '0.1.3';
        }

        console.log('✅ Content viewer extended for 3-column layout (v0.1.3)');
    }

    function addOpenInBrowserButton(shadowRoot) {
        const viewerHeader = shadowRoot.querySelector('.viewer-header');
        if (!viewerHeader) return;
        
        const actionsArea = viewerHeader.querySelector('.viewer-actions');
        if (!actionsArea) return;
        
        // Check if button already exists
        if (shadowRoot.querySelector('#open-in-browser-btn')) return;
        
        // Create the button
        const openBtn = document.createElement('a');
        openBtn.id = 'open-in-browser-btn';
        openBtn.className = 'viewer-action';
        openBtn.innerHTML = '🌐 Open';
        openBtn.title = 'Open in Browser (API endpoint)';
        openBtn.target = '_blank';
        openBtn.rel = 'noopener noreferrer';
        openBtn.style.cssText = `
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 4px 8px;
            font-size: 12px;
            color: var(--color-text-secondary, #8b949e);
            text-decoration: none;
            cursor: pointer;
            border-radius: 4px;
            transition: all 0.15s ease;
        `;
        
        openBtn.addEventListener('mouseenter', () => {
            openBtn.style.color = 'var(--color-accent-primary, #00d9ff)';
            openBtn.style.background = 'rgba(0, 217, 255, 0.1)';
        });
        
        openBtn.addEventListener('mouseleave', () => {
            openBtn.style.color = 'var(--color-text-secondary, #8b949e)';
            openBtn.style.background = 'transparent';
        });
        
        const firstAction = actionsArea.firstChild;
        if (firstAction) {
            actionsArea.insertBefore(openBtn, firstAction);
        } else {
            actionsArea.appendChild(openBtn);
        }
    }

    function updateOpenInBrowserLink(shadowRoot, path) {
        const openBtn = shadowRoot.querySelector('#open-in-browser-btn');
        if (!openBtn) return;
        
        let pathString = path;
        if (typeof path !== 'string') {
            pathString = String(path);
        }
        
        if (pathString.startsWith('/')) {
            pathString = pathString.substring(1);
        }
        
        const encodedPath = encodeURIComponent(pathString);
        const apiUrl = `${API_BASE_URL}/admin/storage/file/content/${encodedPath}`;
        
        openBtn.href = apiUrl;
        openBtn.title = `Open in Browser:\n${apiUrl}`;
    }

    function detectAndShowHtml(contentViewer, path) {
        console.log('🔍 Checking for HTML content in:', path);
        
        // Try to get content from contentViewer
        let content = contentViewer.currentContent;
        
        if (!content) {
            // Try raw content
            try {
                content = JSON.parse(contentViewer.rawContent);
            } catch (e) {
                // Check if raw content is HTML
                if (typeof contentViewer.rawContent === 'string' && isHtmlContent(contentViewer.rawContent)) {
                    showHtmlPanel(contentViewer.rawContent, path, 'raw');
                    return;
                }
            }
        }

        if (content) {
            // Check for html field
            if (typeof content === 'object' && typeof content.html === 'string') {
                showHtmlPanel(content.html, path, 'json-field');
                return;
            }
            
            // Check if content itself is HTML string
            if (typeof content === 'string' && isHtmlContent(content)) {
                showHtmlPanel(content, path, 'raw');
                return;
            }
        }

        // No HTML found - hide panel
        console.log('  ❌ No HTML content found');
        hideHtmlPanel();
    }

    function isHtmlContent(str) {
        if (typeof str !== 'string') return false;
        const trimmed = str.trim();
        return trimmed.startsWith('<!DOCTYPE') || 
               trimmed.startsWith('<!doctype') ||
               trimmed.startsWith('<html') ||
               trimmed.startsWith('<HTML') ||
               (trimmed.startsWith('<') && trimmed.includes('</'));
    }

    function showHtmlPanel(html, path, sourceType) {
        console.log('  ✅ HTML detected, showing panel');
        window.htmlPanelManager?.show(html, path, sourceType);
    }

    function hideHtmlPanel() {
        window.htmlPanelManager?.hide();
    }

})();

// ═══════════════════════════════════════════════════════════════════════════════
// Hide the old HTML tab from v0.1.2 (we use panel now)
// ═══════════════════════════════════════════════════════════════════════════════

(function() {
    // Wait for content-viewer
    const check = setInterval(() => {
        const contentViewer = document.querySelector('content-viewer');
        if (contentViewer && contentViewer.shadowRoot) {
            const htmlTab = contentViewer.shadowRoot.querySelector('[data-tab="html"]');
            if (htmlTab) {
                htmlTab.style.display = 'none !important';
                htmlTab.remove(); // Actually remove it
                clearInterval(check);
            }
        }
    }, 200);
    
    setTimeout(() => clearInterval(check), 5000);
})();
