/* ═══════════════════════════════════════════════════════════════════════════════
   Cache Service Browser - v0.1.1 API Client Extensions
   
   Extends the v0.1.0 CacheApiClient with additional methods needed for v0.1.1
   ═══════════════════════════════════════════════════════════════════════════════ */

(function() {
    // Wait for base API client to be available
    const checkClient = setInterval(() => {
        if (window.cacheApiClient) {
            clearInterval(checkClient);
            extendApiClient();
        }
    }, 50);

    // Give up after 5 seconds
    setTimeout(() => clearInterval(checkClient), 5000);

    function extendApiClient() {
        const client = window.cacheApiClient;

        /**
         * Get hash refs (by-hash reference file content)
         * GET /{namespace}/retrieve/hash/{hash}/refs-hash
         */
        client.retrieveHashRefs = async function(namespace, hash) {
            return this.request(`/${namespace}/retrieve/hash/${hash}/refs-hash`);
        };

        /**
         * Get cache ID for a hash (the latest file ID with this hash)
         * GET /{namespace}/retrieve/hash/{hash}/cache-id
         */
        client.retrieveHashCacheId = async function(namespace, hash) {
            return this.request(`/${namespace}/retrieve/hash/${hash}/cache-id`);
        };

        /**
         * Get content by hash
         * GET /{namespace}/retrieve/hash/{hash}
         */
        client.retrieveByHashContent = async function(namespace, hash) {
            return this.request(`/${namespace}/retrieve/hash/${hash}`);
        };

        /**
         * Get JSON content by hash
         * GET /{namespace}/retrieve/hash/{hash}/json
         */
        client.retrieveByHashJson = async function(namespace, hash) {
            return this.request(`/${namespace}/retrieve/hash/${hash}/json`);
        };

        /**
         * Get metadata by hash
         * GET /{namespace}/retrieve/hash/{hash}/metadata
         */
        client.retrieveByHashMetadata = async function(namespace, hash) {
            return this.request(`/${namespace}/retrieve/hash/${hash}/metadata`);
        };

        /**
         * Check if hash exists
         * GET /{namespace}/exists/hash/{hash}
         */
        client.existsByHash = async function(namespace, hash) {
            return this.request(`/${namespace}/exists/hash/${hash}`);
        };

        console.log('✅ CacheApiClient extended with v0.1.1 methods');
    }
})();
