from osbot_fast_api.api.decorators.route_path                               import route_path
from starlette.responses                                                    import RedirectResponse
from starlette.staticfiles                                                  import StaticFiles
import mgraph_ai_service_cache__web_console
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespaces             import Routes__Namespaces
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Delete      import Routes__Data__Delete
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Exists      import Routes__Data__Exists
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__List        import Routes__Data__List
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Retrieve    import Routes__Data__Retrieve
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store       import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Update      import Routes__Data__Update
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Update      import Routes__File__Update
from mgraph_ai_service_cache.fast_api.routes.test_data.Routes__Test_Data    import Routes__Test_Data
from mgraph_ai_service_cache.fast_api.routes.zip.Routes__Zip                import Routes__Zip
from mgraph_ai_service_cache.service.cache.Cache__Service                   import Cache__Service
from osbot_fast_api.api.routes.Routes__Set_Cookie                           import Routes__Set_Cookie
from osbot_fast_api_serverless.fast_api.Serverless__Fast_API                import Serverless__Fast_API
from mgraph_ai_service_cache.config                                         import FAST_API__TITLE, CACHE_SERVICE__WEB_CONSOLE__MAJOR__VERSION, CACHE_SERVICE__WEB_CONSOLE__LATEST__VERSION, CACHE_SERVICE__WEB_CONSOLE__ROUTE__START_PAGE, CACHE_SERVICE__WEB_CONSOLE__PATH
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
        self.add_routes(Routes__File__Update  )
        self.add_routes(Routes__File__Delete  )
        self.add_routes(Routes__Data__Store   )
        self.add_routes(Routes__Data__Retrieve)
        self.add_routes(Routes__Data__Update  )
        self.add_routes(Routes__Data__Exists  )
        self.add_routes(Routes__Data__List    )
        self.add_routes(Routes__Data__Delete  )
        self.add_routes(Routes__Zip           )
        self.add_routes(Routes__Namespace     )
        self.add_routes(Routes__Namespaces    )
        self.add_routes(Routes__Admin__Storage)
        self.add_routes(Routes__Server        )
        self.add_routes(Routes__Info          )
        self.add_routes(Routes__Set_Cookie    )
        self.add_routes(Routes__Test_Data     )
        self.setup_web_console()

    def add_routes(self, class_routes):
        kwargs = dict(app=self.app())
        if 'cache_service' in class_routes.__annotations__:             # if there is a Type_Safe definiton of cache_service
            kwargs["cache_service"] = self.cache_service                # we will assign it (aka dependency injection)
        class_routes(**kwargs).setup()
        return self


    # todo: refactor to separate class (focused on setting up this static route)
    def setup_web_console(self):


        path_static_folder  = mgraph_ai_service_cache__web_console.path
        path_name           = CACHE_SERVICE__WEB_CONSOLE__PATH
        path_static         = f"/{path_name}"

        major_version       = CACHE_SERVICE__WEB_CONSOLE__MAJOR__VERSION
        latest_version      = CACHE_SERVICE__WEB_CONSOLE__LATEST__VERSION
        start_page          = CACHE_SERVICE__WEB_CONSOLE__ROUTE__START_PAGE

        path_latest_version = f"/{path_name}/{major_version}/{latest_version}/{start_page}.html"
        self.app().mount(path_static, StaticFiles(directory=path_static_folder), name=path_name)


        @route_path(path=f'/{CACHE_SERVICE__WEB_CONSOLE__PATH}')
        def redirect_to_latest():
            return RedirectResponse(url=path_latest_version)

        self.add_route_get(redirect_to_latest)


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


