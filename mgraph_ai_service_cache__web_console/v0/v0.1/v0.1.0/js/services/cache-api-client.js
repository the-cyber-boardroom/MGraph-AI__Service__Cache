/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - API Client
   v0.1.0 - HTTP client for Cache Service API with auth error detection
   
   Features:
   - All Cache Service API endpoints
   - Authentication error detection (401/403)
   - Request/response logging
   - Error handling with custom events
   ═══════════════════════════════════════════════════════════════════════════════ */

class CacheApiClient {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl;
        this.defaultNamespace = 'default';
        this._requestId = 0;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Core HTTP Methods
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Make HTTP request with error handling
     * @param {string} endpoint - API endpoint
     * @param {object} options - Fetch options
     * @returns {Promise<*>} Response data
     */
    async fetch(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;
        const requestId = ++this._requestId;
        const startTime = performance.now();
        
        console.log(`🌐 [${requestId}] ${options.method || 'GET'} ${url}`);
        
        try {
            const response = await fetch(url, {
                ...options,
                headers: {
                    'Accept': 'application/json',
                    ...options.headers
                }
            });
            
            const duration = Math.round(performance.now() - startTime);
            
            // Auth error detection
            if (response.status === 401 || response.status === 403) {
                console.warn(`🔒 [${requestId}] Auth required (${response.status}) - ${duration}ms`);
                this.emit('auth-error', { 
                    status: response.status, 
                    endpoint,
                    message: 'Authentication required'
                });
                throw new AuthError('Authentication required', response.status);
            }
            
            // Other errors
            if (!response.ok) {
                const errorText = await response.text().catch(() => 'Unknown error');
                console.error(`❌ [${requestId}] HTTP ${response.status} - ${duration}ms: ${errorText}`);
                throw new ApiError(`HTTP ${response.status}: ${response.statusText}`, response.status, errorText);
            }
            
            // Parse response
            const contentType = response.headers.get('content-type');
            let data;
            
            if (contentType && contentType.includes('application/json')) {
                data = await response.json();
            } else {
                data = await response.text();
            }
            
            console.log(`✅ [${requestId}] ${response.status} - ${duration}ms`);
            return data;
            
        } catch (error) {
            const duration = Math.round(performance.now() - startTime);
            
            if (error instanceof AuthError || error instanceof ApiError) {
                throw error;
            }
            
            // Network or other errors
            console.error(`❌ [${requestId}] Network error - ${duration}ms:`, error.message);
            this.emit('network-error', { endpoint, error: error.message });
            throw new ApiError(`Network error: ${error.message}`, 0);
        }
    }

    /**
     * POST request with JSON body
     */
    async post(endpoint, data, options = {}) {
        return this.fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            body: JSON.stringify(data),
            ...options
        });
    }

    /**
     * DELETE request
     */
    async delete(endpoint, options = {}) {
        return this.fetch(endpoint, {
            method: 'DELETE',
            ...options
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Info & Health Endpoints
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Health check
     * @returns {Promise<{status: string}>}
     */
    async getHealth() {
        return this.fetch('/info/health');
    }

    /**
     * Server info
     * @returns {Promise<object>}
     */
    async getServerInfo() {
        return this.fetch('/info/server');
    }

    /**
     * Service status
     * @returns {Promise<object>}
     */
    async getStatus() {
        return this.fetch('/info/status');
    }

    /**
     * Dependency versions
     * @returns {Promise<object>}
     */
    async getVersions() {
        return this.fetch('/info/versions');
    }

    /**
     * Storage backend info
     * @returns {Promise<object>}
     */
    async getStorageInfo() {
        return this.fetch('/server/storage/info');
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Namespace Endpoints
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * List all namespaces
     * @returns {Promise<string[]>}
     */
    async getNamespaces() {
        return this.fetch('/namespaces/list');
    }

    /**
     * Get namespace statistics
     * @param {string} namespace
     * @returns {Promise<object>}
     */
    async getStats(namespace = this.defaultNamespace) {
        return this.fetch(`/${namespace}/stats`);
    }

    /**
     * Get all file IDs in namespace
     * @param {string} namespace
     * @returns {Promise<string[]>}
     */
    async getFileIds(namespace = this.defaultNamespace) {
        return this.fetch(`/${namespace}/file-ids`);
    }

    /**
     * Get all file hashes in namespace
     * @param {string} namespace
     * @returns {Promise<string[]>}
     */
    async getFileHashes(namespace = this.defaultNamespace) {
        return this.fetch(`/${namespace}/file-hashes`);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Retrieval Endpoints
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Retrieve cache entry by ID
     * @param {string} namespace
     * @param {string} cacheId
     * @returns {Promise<object>}
     */
    async retrieve(namespace, cacheId) {
        return this.fetch(`/${namespace}/retrieve/${cacheId}`);
    }

    /**
     * Retrieve as specific type
     * @param {string} namespace
     * @param {string} cacheId
     * @param {string} type - 'json' | 'string' | 'binary'
     */
    async retrieveAs(namespace, cacheId, type) {
        return this.fetch(`/${namespace}/retrieve/${cacheId}/${type}`);
    }

    /**
     * Retrieve cache metadata
     * @param {string} namespace
     * @param {string} cacheId
     * @returns {Promise<object>}
     */
    async retrieveMetadata(namespace, cacheId) {
        return this.fetch(`/${namespace}/retrieve/${cacheId}/metadata`);
    }

    /**
     * Retrieve cache refs
     * @param {string} namespace
     * @param {string} cacheId
     * @returns {Promise<object>}
     */
    async retrieveRefs(namespace, cacheId) {
        return this.fetch(`/${namespace}/retrieve/${cacheId}/refs`);
    }

    /**
     * Retrieve all refs for cache entry
     * @param {string} namespace
     * @param {string} cacheId
     * @returns {Promise<object>}
     */
    async retrieveRefsAll(namespace, cacheId) {
        return this.fetch(`/${namespace}/retrieve/${cacheId}/refs/all`);
    }

    /**
     * Retrieve cache config
     * @param {string} namespace
     * @param {string} cacheId
     * @returns {Promise<object>}
     */
    async retrieveConfig(namespace, cacheId) {
        return this.fetch(`/${namespace}/retrieve/${cacheId}/config`);
    }

    /**
     * Retrieve by hash
     * @param {string} namespace
     * @param {string} cacheHash
     * @returns {Promise<object>}
     */
    async retrieveByHash(namespace, cacheHash) {
        return this.fetch(`/${namespace}/retrieve/hash/${cacheHash}`);
    }

    /**
     * Retrieve hash metadata
     * @param {string} namespace
     * @param {string} cacheHash
     * @returns {Promise<object>}
     */
    async retrieveHashMetadata(namespace, cacheHash) {
        return this.fetch(`/${namespace}/retrieve/hash/${cacheHash}/metadata`);
    }

    /**
     * Retrieve hash refs
     * @param {string} namespace
     * @param {string} cacheHash
     * @returns {Promise<object>}
     */
    async retrieveHashRefs(namespace, cacheHash) {
        return this.fetch(`/${namespace}/retrieve/hash/${cacheHash}/refs-hash`);
    }

    /**
     * Check if hash exists
     * @param {string} namespace
     * @param {string} cacheHash
     * @returns {Promise<boolean>}
     */
    async existsByHash(namespace, cacheHash) {
        return this.fetch(`/${namespace}/exists/hash/${cacheHash}`);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Admin Storage Endpoints
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Get storage bucket name
     * @returns {Promise<string>}
     */
    async getBucketName() {
        return this.fetch('/admin/storage/bucket-name');
    }

    /**
     * Check if file exists
     * @param {string} path
     * @returns {Promise<boolean>}
     */
    async fileExists(path) {
        return this.fetch(`/admin/storage/file/exists/${path}`);
    }

    /**
     * Get file as JSON
     * @param {string} path
     * @returns {Promise<object>}
     */
    async getFileJson(path) {
        return this.fetch(`/admin/storage/file/json/${path}`);
    }

    /**
     * Get all files in path
     * @param {string} path
     * @returns {Promise<object>}
     */
    async getAllFiles(path) {
        return this.fetch(`/admin/storage/files/all/${path}`);
    }

    /**
     * Get files in path (with options)
     * @param {string} path
     * @param {boolean} fullPath - Return full paths
     * @param {boolean} recursive - Include subdirectories
     * @returns {Promise<string[]>}
     */
    async getFilesIn(path, fullPath = false, recursive = false) {
        const params = new URLSearchParams();
        if (fullPath) params.set('return_full_path', 'true');
        if (recursive) params.set('recursive', 'true');
        const query = params.toString() ? `?${params.toString()}` : '';
        return this.fetch(`/admin/storage/files/in/${path}${query}`);
    }

    /**
     * Get folders in path
     * @param {string} path
     * @param {boolean} fullPath
     * @param {boolean} recursive
     * @returns {Promise<string[]>}
     */
    async getFolders(path, fullPath = false, recursive = false) {
        const params = new URLSearchParams();
        if (fullPath) params.set('return_full_path', 'true');
        if (recursive) params.set('recursive', 'true');
        const query = params.toString() ? `?${params.toString()}` : '';
        return this.fetch(`/admin/storage/folders/${path}${query}`);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Event Emitter
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Emit custom event on document
     * @param {string} eventName
     * @param {object} detail
     */
    emit(eventName, detail = {}) {
        document.dispatchEvent(new CustomEvent(`cache-api:${eventName}`, {
            detail,
            bubbles: true
        }));
    }

    /**
     * Listen for API events
     * @param {string} eventName
     * @param {Function} handler
     */
    on(eventName, handler) {
        document.addEventListener(`cache-api:${eventName}`, handler);
    }

    /**
     * Remove event listener
     * @param {string} eventName
     * @param {Function} handler
     */
    off(eventName, handler) {
        document.removeEventListener(`cache-api:${eventName}`, handler);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Custom Error Classes
// ═══════════════════════════════════════════════════════════════════════════════

class ApiError extends Error {
    constructor(message, status, details = null) {
        super(message);
        this.name = 'ApiError';
        this.status = status;
        this.details = details;
    }
}

class AuthError extends Error {
    constructor(message, status) {
        super(message);
        this.name = 'AuthError';
        this.status = status;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Global Instance
// ═══════════════════════════════════════════════════════════════════════════════

// Create global instance
window.cacheApiClient = new CacheApiClient();
window.CacheApiClient = CacheApiClient;
window.ApiError = ApiError;
window.AuthError = AuthError;

console.log('✅ CacheApiClient loaded');
