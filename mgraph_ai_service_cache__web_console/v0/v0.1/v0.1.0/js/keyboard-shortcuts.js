/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Keyboard Shortcuts Manager
   v0.1.0 - Event-driven keyboard shortcuts system with dark theme
   
   Features:
   - Loads shortcuts from JSON configuration
   - Dispatches custom events on key press
   - Provides hideable UI panel showing available shortcuts
   - Supports modifier keys (shift, ctrl, alt, meta)
   - Dark theme styling
   
   Events dispatched:
   - shortcut:reload
   - shortcut:maximize-toggle
   - shortcut:namespace-next/prev
   - shortcut:namespace-select (with index in detail)
   - shortcut:tab-next/prev
   - shortcut:item-next/prev
   - shortcut:copy
   - shortcut:search-focus
   - shortcut:help-toggle
   - shortcut:escape
   ═══════════════════════════════════════════════════════════════════════════════ */

function KeyboardShortcuts(options) {
    options = options || {};

    this.configUrl = options.configUrl || '../v0.1.0/data/keyboard-shortcuts.json';
    this.shortcuts = [];
    this.categories = {};
    this.enabled = true;
    this.helpVisible = false;
    this.helpPanel = null;
    this.backdrop = null;

    // Elements where shortcuts should be disabled (text inputs, etc.)
    this.disableInElements = ['INPUT', 'TEXTAREA', 'SELECT'];

    this.init();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Initialization
// ═══════════════════════════════════════════════════════════════════════════════

KeyboardShortcuts.prototype.init = function() {
    this.loadConfig();
    this.bindKeyListener();
    this.createHelpPanel();
    this.bindHelpToggle();
};

KeyboardShortcuts.prototype.loadConfig = function() {
    var self = this;

    fetch(this.configUrl)
        .then(function(response) {
            if (!response.ok) throw new Error('Failed to load shortcuts config');
            return response.json();
        })
        .then(function(config) {
            self.shortcuts = config.shortcuts || [];
            self.categories = config.categories || {};
            self.updateHelpPanel();
            console.log('⌨️ Keyboard shortcuts loaded:', self.shortcuts.length, 'shortcuts');

            // Dispatch event indicating shortcuts are ready
            document.dispatchEvent(new CustomEvent('shortcuts:loaded', {
                detail: { shortcuts: self.shortcuts, categories: self.categories }
            }));
        })
        .catch(function(error) {
            console.warn('Could not load keyboard shortcuts config:', error);
            self.useDefaultShortcuts();
        });
};

KeyboardShortcuts.prototype.useDefaultShortcuts = function() {
    // Fallback defaults if JSON fails to load
    this.shortcuts = [
        { key: 'r', event: 'shortcut:reload', label: 'Reload', category: 'actions' },
        { key: 'm', event: 'shortcut:maximize-toggle', label: 'Maximize', category: 'view' },
        { key: 'n', event: 'shortcut:namespace-next', label: 'Next Namespace', category: 'navigation' },
        { key: 'h', event: 'shortcut:help-toggle', label: 'Help', category: 'help' },
        { key: 'Escape', event: 'shortcut:escape', label: 'Close', category: 'navigation' }
    ];
    this.categories = {
        actions: { label: 'Actions', order: 1 },
        view: { label: 'View', order: 2 },
        navigation: { label: 'Navigation', order: 3 },
        help: { label: 'Help', order: 4 }
    };
    this.updateHelpPanel();
};

// ═══════════════════════════════════════════════════════════════════════════════
// Key Listener
// ═══════════════════════════════════════════════════════════════════════════════

KeyboardShortcuts.prototype.bindKeyListener = function() {
    var self = this;
    document.addEventListener('keydown', function(e) {
        self.handleKeydown(e);
    });
};

KeyboardShortcuts.prototype.handleKeydown = function(e) {
    // Skip if shortcuts disabled
    if (!this.enabled) return;

    // Skip if focused on text input elements
    if (this.disableInElements.includes(e.target.tagName)) return;

    // Skip if in contenteditable
    if (e.target.isContentEditable) return;

    // Find matching shortcut
    var shortcut = this.findMatchingShortcut(e);

    if (shortcut) {
        e.preventDefault();
        e.stopPropagation();
        this.dispatchShortcutEvent(shortcut);
    }
};

KeyboardShortcuts.prototype.findMatchingShortcut = function(e) {
    var key = e.key;

    for (var i = 0; i < this.shortcuts.length; i++) {
        var shortcut = this.shortcuts[i];

        // Check key match (case-sensitive for letters with shift)
        var keyMatch = (shortcut.key === key) ||
                       (shortcut.key.toLowerCase() === key.toLowerCase() && !shortcut.shift);

        if (!keyMatch) continue;

        // Check modifier keys
        var shiftMatch = !!shortcut.shift === e.shiftKey;
        var ctrlMatch = !!shortcut.ctrl === (e.ctrlKey || e.metaKey);
        var altMatch = !!shortcut.alt === e.altKey;

        // For uppercase letters, shift must be pressed
        if (shortcut.shift && shortcut.key === shortcut.key.toUpperCase() &&
            shortcut.key !== shortcut.key.toLowerCase()) {
            shiftMatch = e.shiftKey;
            keyMatch = key === shortcut.key || key.toUpperCase() === shortcut.key;
        }

        if (keyMatch && shiftMatch && ctrlMatch && altMatch) {
            return shortcut;
        }
    }

    return null;
};

KeyboardShortcuts.prototype.dispatchShortcutEvent = function(shortcut) {
    var eventName = shortcut.event;
    var detail = Object.assign({}, shortcut.data || {}, {
        shortcut: shortcut
    });

    console.log('⌨️ Shortcut:', eventName, detail);
    document.dispatchEvent(new CustomEvent(eventName, { detail: detail }));
};

// ═══════════════════════════════════════════════════════════════════════════════
// Help Panel UI (Dark Theme)
// ═══════════════════════════════════════════════════════════════════════════════

KeyboardShortcuts.prototype.createHelpPanel = function() {
    // Create backdrop
    this.backdrop = document.createElement('div');
    this.backdrop.id = 'shortcuts-backdrop';
    this.backdrop.className = 'shortcuts-backdrop';
    document.body.appendChild(this.backdrop);

    // Create panel
    this.helpPanel = document.createElement('div');
    this.helpPanel.id = 'keyboard-shortcuts-help';
    this.helpPanel.className = 'shortcuts-modal';
    this.helpPanel.innerHTML = this.generateHelpHTML();
    document.body.appendChild(this.helpPanel);

    // Add styles
    this.addStyles();

    // Close handlers
    var self = this;
    this.backdrop.addEventListener('click', function() {
        self.hideHelp();
    });
    this.helpPanel.querySelector('.shortcuts-close')?.addEventListener('click', function() {
        self.hideHelp();
    });
};

KeyboardShortcuts.prototype.bindHelpToggle = function() {
    var self = this;
    document.addEventListener('shortcut:help-toggle', function() {
        self.toggleHelp();
    });
    document.addEventListener('shortcut:escape', function() {
        if (self.helpVisible) {
            self.hideHelp();
        }
    });
};

KeyboardShortcuts.prototype.generateHelpHTML = function() {
    var html = '<div class="shortcuts-modal-header">';
    html += '<span class="shortcuts-modal-title">⌨️ Keyboard Shortcuts</span>';
    html += '<button class="shortcuts-close btn btn-ghost btn-icon" aria-label="Close">✕</button>';
    html += '</div>';
    html += '<div class="shortcuts-modal-body">';

    // Group by category
    var grouped = this.groupByCategory();
    var categoryOrder = this.getSortedCategories();

    for (var i = 0; i < categoryOrder.length; i++) {
        var catKey = categoryOrder[i];
        var shortcuts = grouped[catKey];
        if (!shortcuts || shortcuts.length === 0) continue;

        var catLabel = this.categories[catKey]?.label || catKey;

        html += '<div class="shortcuts-category">';
        html += '<div class="shortcuts-category-title">' + catLabel + '</div>';

        for (var j = 0; j < shortcuts.length; j++) {
            var s = shortcuts[j];
            html += '<div class="shortcut-row">';
            html += '<div class="shortcut-info">';
            html += '<span class="shortcut-label">' + s.label + '</span>';
            if (s.description) {
                html += '<span class="shortcut-description">' + s.description + '</span>';
            }
            html += '</div>';
            html += '<div class="shortcut-keys">' + this.formatKeyHTML(s) + '</div>';
            html += '</div>';
        }

        html += '</div>';
    }

    html += '</div>';
    return html;
};

KeyboardShortcuts.prototype.formatKeyHTML = function(shortcut) {
    var parts = [];
    if (shortcut.ctrl) parts.push('<kbd>Ctrl</kbd>');
    if (shortcut.alt) parts.push('<kbd>Alt</kbd>');
    if (shortcut.shift) parts.push('<kbd>Shift</kbd>');

    var key = shortcut.key;
    if (key === 'Escape') key = 'Esc';
    if (key === ' ') key = 'Space';
    if (key === 'Enter') key = '↵';

    parts.push('<kbd>' + key + '</kbd>');
    return parts.join(' ');
};

KeyboardShortcuts.prototype.formatKey = function(shortcut) {
    var parts = [];
    if (shortcut.ctrl) parts.push('Ctrl');
    if (shortcut.alt) parts.push('Alt');
    if (shortcut.shift) parts.push('Shift');

    var key = shortcut.key;
    if (key === 'Escape') key = 'Esc';
    if (key === ' ') key = 'Space';

    parts.push(key);
    return parts.join(' + ');
};

KeyboardShortcuts.prototype.groupByCategory = function() {
    var grouped = {};
    for (var i = 0; i < this.shortcuts.length; i++) {
        var s = this.shortcuts[i];
        var cat = s.category || 'other';
        if (!grouped[cat]) grouped[cat] = [];
        grouped[cat].push(s);
    }
    return grouped;
};

KeyboardShortcuts.prototype.getSortedCategories = function() {
    var self = this;
    var keys = Object.keys(this.categories);
    keys.sort(function(a, b) {
        var orderA = self.categories[a]?.order || 999;
        var orderB = self.categories[b]?.order || 999;
        return orderA - orderB;
    });
    return keys;
};

KeyboardShortcuts.prototype.updateHelpPanel = function() {
    if (this.helpPanel) {
        this.helpPanel.innerHTML = this.generateHelpHTML();
        // Re-bind close button
        var self = this;
        this.helpPanel.querySelector('.shortcuts-close')?.addEventListener('click', function() {
            self.hideHelp();
        });
    }
};

KeyboardShortcuts.prototype.showHelp = function() {
    if (this.helpPanel && this.backdrop) {
        this.backdrop.classList.add('show');
        this.helpPanel.classList.add('show');
        this.helpVisible = true;
    }
};

KeyboardShortcuts.prototype.hideHelp = function() {
    if (this.helpPanel && this.backdrop) {
        this.backdrop.classList.remove('show');
        this.helpPanel.classList.remove('show');
        this.helpVisible = false;
    }
};

KeyboardShortcuts.prototype.toggleHelp = function() {
    if (this.helpVisible) {
        this.hideHelp();
    } else {
        this.showHelp();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Dark Theme Styles
// ═══════════════════════════════════════════════════════════════════════════════

KeyboardShortcuts.prototype.addStyles = function() {
    if (document.getElementById('keyboard-shortcuts-styles')) return;

    var style = document.createElement('style');
    style.id = 'keyboard-shortcuts-styles';
    style.textContent = `
        .shortcuts-backdrop {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.6);
            backdrop-filter: blur(2px);
            z-index: 299;
            display: none;
        }
        
        .shortcuts-backdrop.show {
            display: block;
        }
        
        .shortcuts-modal {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 90%;
            max-width: 560px;
            max-height: 80vh;
            background: var(--color-bg-secondary, #161b22);
            border: 1px solid var(--color-border-default, #30363d);
            border-radius: var(--radius-xl, 12px);
            box-shadow: var(--shadow-lg, 0 8px 16px rgba(0, 0, 0, 0.5));
            z-index: 300;
            overflow: hidden;
            display: none;
        }
        
        .shortcuts-modal.show {
            display: block;
        }
        
        .shortcuts-modal-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: var(--spacing-md, 16px) var(--spacing-lg, 24px);
            border-bottom: 1px solid var(--color-border-default, #30363d);
            background: var(--color-bg-tertiary, #21262d);
        }
        
        .shortcuts-modal-title {
            font-size: var(--font-size-lg, 1.125rem);
            font-weight: 600;
            color: var(--color-text-primary, #e6edf3);
        }
        
        .shortcuts-modal-body {
            padding: var(--spacing-lg, 24px);
            overflow-y: auto;
            max-height: calc(80vh - 60px);
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: var(--spacing-lg, 24px);
        }
        
        .shortcuts-category {
            display: flex;
            flex-direction: column;
        }
        
        .shortcuts-category-title {
            font-size: var(--font-size-xs, 0.75rem);
            font-weight: 600;
            color: var(--color-text-muted, #6e7681);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: var(--spacing-sm, 8px);
            padding-bottom: var(--spacing-xs, 4px);
            border-bottom: 1px solid var(--color-border-muted, #21262d);
        }
        
        .shortcut-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: var(--spacing-sm, 8px) 0;
            gap: var(--spacing-md, 16px);
        }
        
        .shortcut-info {
            display: flex;
            flex-direction: column;
            gap: 2px;
            min-width: 0;
        }
        
        .shortcut-label {
            color: var(--color-text-primary, #e6edf3);
            font-size: var(--font-size-sm, 0.875rem);
        }
        
        .shortcut-description {
            font-size: var(--font-size-xs, 0.75rem);
            color: var(--color-text-muted, #6e7681);
        }
        
        .shortcut-keys {
            display: flex;
            gap: var(--spacing-xs, 4px);
            flex-shrink: 0;
        }
        
        .shortcut-keys kbd {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 22px;
            height: 22px;
            padding: 0 var(--spacing-sm, 8px);
            font-family: var(--font-family-mono, monospace);
            font-size: var(--font-size-xs, 0.75rem);
            font-weight: 500;
            color: var(--color-text-secondary, #8b949e);
            background: var(--color-bg-tertiary, #21262d);
            border: 1px solid var(--color-border-default, #30363d);
            border-radius: var(--radius-sm, 4px);
            box-shadow: inset 0 -1px 0 var(--color-border-default, #30363d);
        }
    `;
    document.head.appendChild(style);
};

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

KeyboardShortcuts.prototype.enable = function() {
    this.enabled = true;
};

KeyboardShortcuts.prototype.disable = function() {
    this.enabled = false;
};

KeyboardShortcuts.prototype.isEnabled = function() {
    return this.enabled;
};

KeyboardShortcuts.prototype.getShortcuts = function() {
    return this.shortcuts;
};

KeyboardShortcuts.prototype.getShortcutForEvent = function(eventName) {
    for (var i = 0; i < this.shortcuts.length; i++) {
        if (this.shortcuts[i].event === eventName) {
            return this.shortcuts[i];
        }
    }
    return null;
};

// Export
if (typeof module !== 'undefined' && module.exports) {
    module.exports = KeyboardShortcuts;
}
if (typeof window !== 'undefined') {
    window.KeyboardShortcuts = KeyboardShortcuts;
}

console.log('✅ KeyboardShortcuts loaded');
