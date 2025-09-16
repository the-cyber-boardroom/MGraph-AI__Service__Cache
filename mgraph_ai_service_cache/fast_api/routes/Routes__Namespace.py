from typing import Dict, Any
from osbot_fast_api.api.routes.Fast_API__Routes                     import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag          import Safe_Str__Fast_API__Route__Tag
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id   import Safe_Id
from mgraph_ai_service_cache.service.cache.Cache__Service           import Cache__Service

TAG__ROUTES_NAMESPACE                  = 'namespace'
PREFIX__ROUTES_NAMESPACE               = '/{namespace}'
ROUTES_PATHS__NAMESPACE                = [ PREFIX__ROUTES_NAMESPACE + '/stats']


class Routes__Namespace(Fast_API__Routes):
    tag           : Safe_Str__Fast_API__Route__Tag     = TAG__ROUTES_NAMESPACE
    prefix        : Safe_Str__Fast_API__Route__Prefix  =  PREFIX__ROUTES_NAMESPACE
    cache_service : Cache__Service



    def stats(self, namespace: Safe_Id = None) -> Dict[str, Any]:       # Get cache statistics
        namespace = namespace or Safe_Id("default")

        try:
            # Get file counts using shared method
            counts_data = self.cache_service.get_namespace_file_counts(namespace)
            handler = counts_data['handler']

            # Build stats response
            stats = { "namespace": str(namespace)         ,
                      "s3_bucket": handler.s3__bucket     ,
                      "s3_prefix": handler.s3__prefix     ,
                      "ttl_hours": handler.cache_ttl_hours,
                      **counts_data['file_counts'        ]}  # Spread all the file counts

            return stats
        except Exception as e:
            return {"error": str(e), "namespace": str(namespace)}

    def setup_routes(self):
        self.add_route_get(self.stats)