from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Delete      import Routes__Data__Delete
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Retrieve    import Routes__Data__Retrieve
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store       import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.zip.Routes__Zip                import Routes__Zip
from mgraph_ai_service_cache.service.cache.Cache__Service                   import Cache__Service
from osbot_fast_api.api.routes.Routes__Set_Cookie                           import Routes__Set_Cookie
from osbot_fast_api_serverless.fast_api.Serverless__Fast_API                import Serverless__Fast_API
from mgraph_ai_service_cache.config                                         import FAST_API__TITLE
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Delete      import Routes__File__Delete
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Exists      import Routes__File__Exists
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Retrieve    import Routes__File__Retrieve
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store       import Routes__File__Store
from mgraph_ai_service_cache.fast_api.routes.Routes__Info                   import Routes__Info
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespace              import Routes__Namespace
from mgraph_ai_service_cache.fast_api.routes.Routes__Server                 import Routes__Server
from mgraph_ai_service_cache.fast_api.routes.admin.Routes__Admin__Storage   import Routes__Admin__Storage
from mgraph_ai_service_cache.utils.Version                                  import version__mgraph_ai_service_cache

class Cache_Service__Fast_API(Serverless__Fast_API):
    cache_service: Cache__Service

    def setup(self):
        with self.config as _:
            _.title  = FAST_API__TITLE
            _.version = version__mgraph_ai_service_cache
        return super().setup()

    def setup_routes(self):
        self.add_routes(Routes__File__Store   )
        self.add_routes(Routes__File__Retrieve)
        self.add_routes(Routes__File__Exists  )
        self.add_routes(Routes__File__Delete  )
        self.add_routes(Routes__Data__Store   )
        self.add_routes(Routes__Data__Retrieve)
        self.add_routes(Routes__Data__Delete  )
        self.add_routes(Routes__Zip           )
        self.add_routes(Routes__Namespace     )
        self.add_routes(Routes__Admin__Storage)
        self.add_routes(Routes__Server        )
        self.add_routes(Routes__Info          )
        self.add_routes(Routes__Set_Cookie    )

    def add_routes(self, class_routes):
        kwargs = dict(app=self.app())
        if 'cache_service' in class_routes.__annotations__:             # if there is a Type_Safe definiton of cache_service
            kwargs["cache_service"] = self.cache_service                # we will assign it (aka dependency injection)
        class_routes(**kwargs).setup()
        return self

    # def setup_middlewares(self):
    #     super().setup_middlewares()
    #
    #     return self


# # todo: see if we need to refactor this into Fast_API since the current routes are working ok for this project (for string, bytes and dict)
# intercept the bytes submitted here
#self.app().add_middleware(BodyReaderMiddleware)

# from fastapi import Request
# from starlette.middleware.base import BaseHTTPMiddleware
# class BodyReaderMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request: Request, call_next):
#         if request.method in ["POST", "PUT", "PATCH"]:                              # Only read body for certain methods
#             body = await request.body()
#             request.state.body = body                                               # Store it in request state for later sync access
#         else:
#             request.state.body = None
#
#         response = await call_next(request)
#         return response


