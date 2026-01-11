/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - Helper Utilities
   v0.1.0 - Common utility functions
   ═══════════════════════════════════════════════════════════════════════════════ */

const Helpers = {
    /**
     * Format bytes to human readable string
     * @param {number} bytes - Number of bytes
     * @param {number} decimals - Decimal places (default: 1)
     * @returns {string} Formatted string (e.g., "1.5 KB")
     */
    formatBytes(bytes, decimals = 1) {
        if (bytes === 0) return '0 B';
        if (!bytes || isNaN(bytes)) return '-';
        
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        
        return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i];
    },

    /**
     * Format number with commas
     * @param {number} num - Number to format
     * @returns {string} Formatted number (e.g., "1,234,567")
     */
    formatNumber(num) {
        if (num === null || num === undefined) return '-';
        return num.toLocaleString();
    },

    /**
     * Format date to locale string
     * @param {string|Date|number} date - Date to format
     * @param {object} options - Intl.DateTimeFormat options
     * @returns {string} Formatted date
     */
    formatDate(date, options = {}) {
        if (!date) return '-';
        
        const d = date instanceof Date ? date : new Date(date);
        if (isNaN(d.getTime())) return '-';
        
        const defaultOptions = {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            ...options
        };
        
        return d.toLocaleString(undefined, defaultOptions);
    },

    /**
     * Format relative time (e.g., "2 hours ago")
     * @param {string|Date|number} date - Date to format
     * @returns {string} Relative time string
     */
    formatRelativeTime(date) {
        if (!date) return '-';
        
        const d = date instanceof Date ? date : new Date(date);
        if (isNaN(d.getTime())) return '-';
        
        const now = new Date();
        const diffMs = now - d;
        const diffSec = Math.floor(diffMs / 1000);
        const diffMin = Math.floor(diffSec / 60);
        const diffHour = Math.floor(diffMin / 60);
        const diffDay = Math.floor(diffHour / 24);
        
        if (diffSec < 60) return 'just now';
        if (diffMin < 60) return `${diffMin}m ago`;
        if (diffHour < 24) return `${diffHour}h ago`;
        if (diffDay < 7) return `${diffDay}d ago`;
        
        return this.formatDate(d, { year: 'numeric', month: 'short', day: 'numeric' });
    },

    /**
     * Escape HTML special characters
     * @param {string} text - Text to escape
     * @returns {string} Escaped text
     */
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    },

    /**
     * Truncate string with ellipsis
     * @param {string} str - String to truncate
     * @param {number} maxLength - Maximum length
     * @returns {string} Truncated string
     */
    truncate(str, maxLength = 50) {
        if (!str || str.length <= maxLength) return str || '';
        return str.substring(0, maxLength - 3) + '...';
    },

    /**
     * Copy text to clipboard
     * @param {string} text - Text to copy
     * @returns {Promise<boolean>} Success status
     */
    async copyToClipboard(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            // Fallback for older browsers
            const textarea = document.createElement('textarea');
            textarea.value = text;
            textarea.style.position = 'fixed';
            textarea.style.opacity = '0';
            document.body.appendChild(textarea);
            textarea.select();
            
            try {
                document.execCommand('copy');
                return true;
            } catch (e) {
                console.error('Copy failed:', e);
                return false;
            } finally {
                document.body.removeChild(textarea);
            }
        }
    },

    /**
     * Debounce function
     * @param {Function} func - Function to debounce
     * @param {number} wait - Wait time in ms
     * @returns {Function} Debounced function
     */
    debounce(func, wait = 250) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    /**
     * Throttle function
     * @param {Function} func - Function to throttle
     * @param {number} limit - Time limit in ms
     * @returns {Function} Throttled function
     */
    throttle(func, limit = 100) {
        let inThrottle;
        return function executedFunction(...args) {
            if (!inThrottle) {
                func(...args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    },

    /**
     * Check if string is valid URL
     * @param {string} str - String to check
     * @returns {boolean} Is valid URL
     */
    isValidUrl(str) {
        try {
            new URL(str);
            return true;
        } catch {
            return false;
        }
    },

    /**
     * Get file extension from path
     * @param {string} path - File path
     * @returns {string} Extension (lowercase, without dot)
     */
    getExtension(path) {
        if (!path) return '';
        const parts = path.split('.');
        return parts.length > 1 ? parts.pop().toLowerCase() : '';
    },

    /**
     * Get filename from path
     * @param {string} path - File path
     * @returns {string} Filename
     */
    getFilename(path) {
        if (!path) return '';
        return path.split('/').pop();
    },

    /**
     * Detect content type from data
     * @param {*} data - Data to check
     * @returns {string} Content type: 'json' | 'html' | 'image' | 'text' | 'binary'
     */
    detectContentType(data) {
        if (data === null || data === undefined) return 'text';
        
        // Object/Array → JSON
        if (typeof data === 'object') return 'json';
        
        // String analysis
        if (typeof data === 'string') {
            const trimmed = data.trim();
            
            // Check for JSON
            if ((trimmed.startsWith('{') && trimmed.endsWith('}')) ||
                (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
                try {
                    JSON.parse(trimmed);
                    return 'json';
                } catch {}
            }
            
            // Check for HTML
            if (trimmed.startsWith('<!DOCTYPE') || 
                trimmed.startsWith('<html') ||
                /<[a-z][\s\S]*>/i.test(trimmed)) {
                return 'html';
            }
            
            // Check for base64 image
            if (trimmed.startsWith('data:image/')) {
                return 'image';
            }
            
            return 'text';
        }
        
        return 'binary';
    },

    /**
     * Pretty print JSON with syntax highlighting
     * @param {*} obj - Object to format
     * @param {number} indent - Indentation level
     * @returns {string} HTML string with syntax highlighting
     */
    formatJson(obj, indent = 2) {
        const json = typeof obj === 'string' ? obj : JSON.stringify(obj, null, indent);
        
        return json
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"([^"]+)":/g, '<span class="json-key">"$1"</span>:')
            .replace(/: "([^"]*)"/g, ': <span class="json-string">"$1"</span>')
            .replace(/: (\d+\.?\d*)/g, ': <span class="json-number">$1</span>')
            .replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>')
            .replace(/: (null)/g, ': <span class="json-null">$1</span>');
    },

    /**
     * Format HTML with indentation
     * @param {string} html - HTML string
     * @returns {string} Formatted HTML
     */
    formatHtml(html) {
        if (!html) return '';
        
        let formatted = '';
        let indent = 0;
        const tab = '  ';
        
        // Simple regex-based formatting
        html = html.replace(/>\s*</g, '><');
        
        const tokens = html.split(/(<[^>]+>)/);
        
        for (const token of tokens) {
            if (!token.trim()) continue;
            
            // Closing tag
            if (token.match(/^<\//)) {
                indent--;
                formatted += tab.repeat(Math.max(0, indent)) + token + '\n';
            }
            // Self-closing or void tag
            else if (token.match(/\/>$/) || token.match(/^<(area|base|br|col|embed|hr|img|input|link|meta|param|source|track|wbr)/i)) {
                formatted += tab.repeat(indent) + token + '\n';
            }
            // Opening tag
            else if (token.match(/^</)) {
                formatted += tab.repeat(indent) + token + '\n';
                indent++;
            }
            // Text content
            else {
                formatted += tab.repeat(indent) + token.trim() + '\n';
            }
        }
        
        return formatted.trim();
    },

    /**
     * Generate a unique ID
     * @param {string} prefix - Optional prefix
     * @returns {string} Unique ID
     */
    generateId(prefix = 'id') {
        return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).substr(2, 9)}`;
    },

    /**
     * Deep clone an object
     * @param {*} obj - Object to clone
     * @returns {*} Cloned object
     */
    deepClone(obj) {
        return JSON.parse(JSON.stringify(obj));
    },

    /**
     * Group array by key
     * @param {Array} array - Array to group
     * @param {string|Function} key - Key to group by
     * @returns {Object} Grouped object
     */
    groupBy(array, key) {
        return array.reduce((result, item) => {
            const groupKey = typeof key === 'function' ? key(item) : item[key];
            (result[groupKey] = result[groupKey] || []).push(item);
            return result;
        }, {});
    },

    /**
     * Parse file path into components
     * @param {string} path - File path
     * @returns {Object} Path components { namespace, strategy, type, hash, filename }
     */
    parseFilePath(path) {
        if (!path) return {};
        
        const parts = path.split('/');
        const result = {
            full: path,
            namespace: parts[0] || null,
            parts: parts
        };
        
        // Detect path type
        if (path.includes('/refs/by-hash/')) {
            result.type = 'ref-hash';
            result.hash = this.getFilename(path).replace('.json', '');
        } else if (path.includes('/refs/by-id/')) {
            result.type = 'ref-id';
            result.cacheId = this.getFilename(path).replace('.json', '');
        } else if (path.includes('/data/')) {
            result.type = 'data';
            // Extract strategy
            const strategies = ['direct', 'temporal', 'temporal-latest', 'temporal-versioned', 'key-based'];
            for (const strategy of strategies) {
                if (path.includes(`/data/${strategy}`)) {
                    result.strategy = strategy;
                    break;
                }
            }
        }
        
        result.filename = this.getFilename(path);
        result.extension = this.getExtension(path);
        
        return result;
    }
};

// Export for ES6 modules and global access
if (typeof module !== 'undefined' && module.exports) {
    module.exports = Helpers;
}
if (typeof window !== 'undefined') {
    window.Helpers = Helpers;
}

console.log('✅ Helpers loaded');
