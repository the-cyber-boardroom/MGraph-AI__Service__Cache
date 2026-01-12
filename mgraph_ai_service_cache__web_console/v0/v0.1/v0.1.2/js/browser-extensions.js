/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.2 Orchestrator Extensions
   
   - Adds HTML view tab to content-viewer for JSON with 'html' field
   - Extends file loading to detect HTML content
   - Adds "Open in Browser" link
   - Fixes JSON string truncation
   ═══════════════════════════════════════════════════════════════════════════════ */

(function() {
    // Configuration
    const API_BASE_URL = 'http://localhost:10017';
    
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

        // Create HTML tab and viewer in shadow DOM
        addHtmlTabToViewer(shadowRoot);
        
        // Add "Open in Browser" button
        addOpenInBrowserButton(shadowRoot);
        
        // Fix JSON truncation
        fixJsonTruncation(shadowRoot);

        // Override loadFile to detect HTML and update Open in Browser link
        contentViewer.loadFile = async function(pathOrDetail, options = {}) {
            // Extract path string - loadFile receives e.detail which is { path, type, namespace }
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
            
            // Store the path for later use
            contentViewer._currentPath = pathString;
            
            console.log('📂 Loading file:', pathString);
            
            // Call original with the original argument
            await originalLoadFile(pathOrDetail, options);
            
            // Update Open in Browser link with the string path
            updateOpenInBrowserLink(shadowRoot, pathString);
            
            // After loading, check for HTML content
            // Use longer timeout to ensure content is fully rendered
            setTimeout(() => {
                checkForHtmlContent(contentViewer, shadowRoot, pathString);
            }, 300);
        };

        // Also listen for content changes to detect HTML after JSON is rendered
        setTimeout(() => {
            const jsonContent = shadowRoot.querySelector('#json-content, .json-content');
            if (jsonContent) {
                const observer = new MutationObserver(() => {
                    if (contentViewer._currentPath) {
                        // Debounce the check
                        clearTimeout(contentViewer._htmlCheckTimeout);
                        contentViewer._htmlCheckTimeout = setTimeout(() => {
                            checkForHtmlContent(contentViewer, shadowRoot, contentViewer._currentPath);
                        }, 200);
                    }
                });
                observer.observe(jsonContent, { childList: true, subtree: true, characterData: true });
                console.log('👀 Watching JSON content for changes');
            }
        }, 500);

        // Update version
        if (window.cacheBrowser) {
            window.cacheBrowser.version = '0.1.2';
        }

        console.log('✅ Content viewer extended with HTML detection + Open in Browser (v0.1.2)');
    }

    function addOpenInBrowserButton(shadowRoot) {
        // Find the viewer header actions area
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
        
        // Insert before the first action or at the start
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
        
        // Ensure path is a string
        let pathString = path;
        if (typeof path !== 'string') {
            console.warn('Open in Browser: path is not a string:', path);
            pathString = String(path);
        }
        
        // Remove leading slash if present
        if (pathString.startsWith('/')) {
            pathString = pathString.substring(1);
        }
        
        // Encode the path for URL (encode slashes too for the path parameter)
        const encodedPath = encodeURIComponent(pathString);
        const apiUrl = `${API_BASE_URL}/admin/storage/file/json/${encodedPath}`;
        
        openBtn.href = apiUrl;
        openBtn.title = `Open in Browser:\n${apiUrl}`;
        
        console.log('📎 Open in Browser URL:', apiUrl);
    }

    function fixJsonTruncation(shadowRoot) {
        // Add a style element to override truncation
        const style = document.createElement('style');
        style.textContent = `
            /* Remove string truncation */
            .json-string {
                max-width: none !important;
                overflow: visible !important;
                text-overflow: unset !important;
                white-space: pre-wrap !important;
                word-break: break-word !important;
            }
            
            /* Ensure values are fully displayed */
            .json-value {
                max-width: none !important;
                overflow: visible !important;
            }
            
            /* JSON content area should scroll */
            .json-content {
                overflow: auto !important;
            }
            
            /* Remove any line clamping */
            .json-content * {
                -webkit-line-clamp: unset !important;
            }
        `;
        shadowRoot.appendChild(style);
    }

    function addHtmlTabToViewer(shadowRoot) {
        // Find the tabs container
        const tabsContainer = shadowRoot.querySelector('.viewer-tabs');
        if (!tabsContainer) return;

        // Check if HTML tab already exists
        if (shadowRoot.querySelector('[data-tab="html"]')) return;

        // Add HTML tab (initially hidden)
        const htmlTab = document.createElement('button');
        htmlTab.className = 'viewer-tab';
        htmlTab.dataset.tab = 'html';
        htmlTab.textContent = '🌐 HTML';
        htmlTab.style.display = 'none';
        htmlTab.title = 'View as rendered HTML';
        
        // Insert after 'formatted' tab
        const formattedTab = tabsContainer.querySelector('[data-tab="formatted"]');
        if (formattedTab) {
            formattedTab.after(htmlTab);
        } else {
            tabsContainer.appendChild(htmlTab);
        }

        // Add HTML content panel (initially hidden)
        const viewerBody = shadowRoot.querySelector('.viewer-body');
        if (viewerBody) {
            const htmlPanel = document.createElement('div');
            htmlPanel.className = 'viewer-panel';
            htmlPanel.id = 'html-panel';
            htmlPanel.style.display = 'none';
            htmlPanel.innerHTML = `
                <div class="html-viewer-inline" style="height: 100%; display: flex; flex-direction: column;">
                    <div class="html-viewer-toolbar" style="display: flex; align-items: center; gap: 8px; padding: 8px 12px; background: var(--color-bg-tertiary, #21262d); border-bottom: 1px solid var(--color-border-muted, #30363d);">
                        <button class="html-mode-btn active" data-mode="preview" style="padding: 4px 8px; font-size: 12px; background: rgba(0,217,255,0.1); border: 1px solid var(--color-accent-primary, #00d9ff); border-radius: 4px; color: var(--color-accent-primary, #00d9ff); cursor: pointer;">🌐 Preview</button>
                        <button class="html-mode-btn" data-mode="source" style="padding: 4px 8px; font-size: 12px; background: transparent; border: 1px solid transparent; border-radius: 4px; color: var(--color-text-secondary, #8b949e); cursor: pointer;">📄 Source</button>
                        <span style="flex: 1;"></span>
                        <button class="html-action-btn" data-action="copy" style="padding: 4px 8px; font-size: 12px; background: transparent; border: none; color: var(--color-text-secondary, #8b949e); cursor: pointer;">📋 Copy</button>
                        <button class="html-action-btn" data-action="maximize" style="padding: 4px 8px; font-size: 12px; background: transparent; border: none; color: var(--color-text-secondary, #8b949e); cursor: pointer;">⛶ Maximize</button>
                    </div>
                    <div class="html-viewer-content" style="flex: 1; overflow: hidden;">
                        <iframe id="html-preview-iframe" sandbox="allow-same-origin" style="width: 100%; height: 100%; border: none; background: white;"></iframe>
                        <div id="html-source-view" style="display: none; width: 100%; height: 100%; overflow: auto; padding: 12px; background: var(--color-bg-primary, #0d1117);">
                            <pre style="margin: 0; white-space: pre-wrap; font-family: monospace; font-size: 13px; line-height: 1.5; color: var(--color-text-primary, #e6edf3);"></pre>
                        </div>
                    </div>
                </div>
            `;
            viewerBody.appendChild(htmlPanel);

            // Bind HTML panel events
            setupHtmlPanelEvents(shadowRoot, htmlPanel);
        }

        // Add click handler to HTML tab
        htmlTab.addEventListener('click', () => {
            // Deactivate all tabs
            shadowRoot.querySelectorAll('.viewer-tab').forEach(t => t.classList.remove('active'));
            htmlTab.classList.add('active');

            // Hide all panels, show HTML panel
            shadowRoot.querySelectorAll('.viewer-panel').forEach(p => p.style.display = 'none');
            const htmlPanel = shadowRoot.querySelector('#html-panel');
            if (htmlPanel) htmlPanel.style.display = 'block';
        });
    }

    function setupHtmlPanelEvents(shadowRoot, htmlPanel) {
        const modeButtons = htmlPanel.querySelectorAll('.html-mode-btn');
        const actionButtons = htmlPanel.querySelectorAll('.html-action-btn');
        const iframe = htmlPanel.querySelector('#html-preview-iframe');
        const sourceView = htmlPanel.querySelector('#html-source-view');

        modeButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                modeButtons.forEach(b => {
                    b.classList.remove('active');
                    b.style.background = 'transparent';
                    b.style.border = '1px solid transparent';
                    b.style.color = 'var(--color-text-secondary, #8b949e)';
                });
                btn.classList.add('active');
                btn.style.background = 'rgba(0,217,255,0.1)';
                btn.style.border = '1px solid var(--color-accent-primary, #00d9ff)';
                btn.style.color = 'var(--color-accent-primary, #00d9ff)';

                if (btn.dataset.mode === 'preview') {
                    iframe.style.display = 'block';
                    sourceView.style.display = 'none';
                } else {
                    iframe.style.display = 'none';
                    sourceView.style.display = 'block';
                }
            });
        });

        actionButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const html = htmlPanel._currentHtml || '';
                if (btn.dataset.action === 'copy') {
                    Helpers.copyToClipboard(html);
                    btn.textContent = '✓ Copied!';
                    setTimeout(() => btn.textContent = '📋 Copy', 1500);
                } else if (btn.dataset.action === 'maximize') {
                    window.htmlViewerModal?.show(html, htmlPanel._currentPath || '');
                }
            });
        });
    }

    function checkForHtmlContent(contentViewer, shadowRoot, path) {
        console.log('🔍 Checking for HTML content in:', path);
        
        // Try multiple ways to get the content
        let content = null;
        
        // Method 1: Direct access to viewer's stored content
        if (contentViewer.currentContent) {
            content = contentViewer.currentContent;
            console.log('  → Found via currentContent');
        } else if (contentViewer._currentContent) {
            content = contentViewer._currentContent;
            console.log('  → Found via _currentContent');
        }
        
        // Method 2: Try to get from raw content
        if (!content && contentViewer.rawContent) {
            try {
                content = JSON.parse(contentViewer.rawContent);
                console.log('  → Parsed from rawContent');
            } catch (e) {
                // rawContent might be HTML directly
                if (typeof contentViewer.rawContent === 'string' && HtmlDetector.isHtmlContent(contentViewer.rawContent)) {
                    showHtmlTab(shadowRoot, contentViewer.rawContent, path, 'raw');
                    return;
                }
            }
        }
        
        // Method 3: Try to parse from the Raw tab content
        if (!content) {
            const rawText = shadowRoot.querySelector('#raw-text, .raw-content pre');
            if (rawText && rawText.textContent) {
                try {
                    content = JSON.parse(rawText.textContent);
                    console.log('  → Parsed from raw tab');
                } catch (e) {
                    // Check if it's raw HTML
                    if (HtmlDetector.isHtmlContent(rawText.textContent)) {
                        showHtmlTab(shadowRoot, rawText.textContent, path, 'raw');
                        return;
                    }
                }
            }
        }
        
        // Method 4: Look for the JSON content in the formatted view
        if (!content) {
            const jsonViewer = shadowRoot.querySelector('#json-content, .json-content');
            if (jsonViewer) {
                // Try to reconstruct from the JSON viewer's text
                const fullText = jsonViewer.textContent;
                try {
                    // Clean up the text (remove toggle characters, etc.)
                    const cleanText = fullText.replace(/[▼▶]/g, '').replace(/\s*\.\.\.\s*\d+\s*(keys|items)\s*[}\]]/g, '');
                    content = JSON.parse(cleanText);
                    console.log('  → Parsed from JSON viewer text');
                } catch (e) {
                    // Try a simpler approach - look for "html": pattern
                    const htmlMatch = fullText.match(/"html"\s*:\s*"([^"]*(?:\\.[^"]*)*)"/);
                    if (htmlMatch) {
                        try {
                            // Unescape the JSON string
                            const htmlContent = JSON.parse('"' + htmlMatch[1] + '"');
                            showHtmlTab(shadowRoot, htmlContent, path, 'json-field');
                            console.log('  → Extracted html field via regex');
                            return;
                        } catch (e2) {
                            console.log('  → Failed to parse extracted html');
                        }
                    }
                }
            }
        }

        // Now check if content has HTML
        if (content) {
            const htmlData = HtmlDetector.extractHtml(content);
            if (htmlData) {
                console.log('  ✅ HTML detected, showing tab');
                showHtmlTab(shadowRoot, htmlData.html, path, htmlData.sourceType);
                return;
            }
        }
        
        // No HTML found, hide the tab
        console.log('  ❌ No HTML content found');
        hideHtmlTab(shadowRoot);
    }

    function showHtmlTab(shadowRoot, html, path, sourceType) {
        const htmlTab = shadowRoot.querySelector('[data-tab="html"]');
        const htmlPanel = shadowRoot.querySelector('#html-panel');
        
        if (!htmlTab || !htmlPanel) return;

        // Show the tab
        htmlTab.style.display = 'inline-flex';
        
        // Add indicator if HTML was extracted from JSON field
        if (sourceType === 'json-field') {
            htmlTab.innerHTML = '🌐 HTML <span style="font-size: 10px; background: rgba(210,153,34,0.2); padding: 1px 4px; border-radius: 2px; margin-left: 4px;">field</span>';
        } else {
            htmlTab.textContent = '🌐 HTML';
        }

        // Store HTML in panel for actions
        htmlPanel._currentHtml = html;
        htmlPanel._currentPath = path;

        // Update iframe and source
        const iframe = htmlPanel.querySelector('#html-preview-iframe');
        const sourceView = htmlPanel.querySelector('#html-source-view pre');
        
        if (iframe) iframe.srcdoc = html;
        if (sourceView) sourceView.textContent = html;
    }

    function hideHtmlTab(shadowRoot) {
        const htmlTab = shadowRoot.querySelector('[data-tab="html"]');
        if (htmlTab) {
            htmlTab.style.display = 'none';
        }
    }

})();
