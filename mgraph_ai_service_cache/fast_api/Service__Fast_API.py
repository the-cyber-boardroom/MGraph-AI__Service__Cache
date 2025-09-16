from osbot_fast_api.api.routes.Routes__Set_Cookie               import Routes__Set_Cookie
from osbot_fast_api_serverless.fast_api.Serverless__Fast_API    import Serverless__Fast_API
from mgraph_ai_service_cache.config                             import FAST_API__TITLE
from mgraph_ai_service_cache.fast_api.routes.Routes__Cache      import Routes__Cache
from mgraph_ai_service_cache.fast_api.routes.Routes__Delete     import Routes__Delete
from mgraph_ai_service_cache.fast_api.routes.Routes__Exists     import Routes__Exists
from mgraph_ai_service_cache.fast_api.routes.Routes__Info       import Routes__Info
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespace  import Routes__Namespace
from mgraph_ai_service_cache.fast_api.routes.Routes__Retrieve   import Routes__Retrieve
from mgraph_ai_service_cache.fast_api.routes.Routes__Server     import Routes__Server
from mgraph_ai_service_cache.fast_api.routes.Routes__Store      import Routes__Store
from mgraph_ai_service_cache.utils.Version                      import version__mgraph_ai_service_cache



class Service__Fast_API(Serverless__Fast_API):

    def fast_api__title(self):                                       # todo: move this to the Fast_API class
        return FAST_API__TITLE

    def setup(self):
        super().setup()
        self.setup_fast_api_title_and_version()
        return self

    def setup_fast_api_title_and_version(self):                     # todo: move this to the Fast_API class
        app       = self.app()
        app.title = self.fast_api__title()
        app.version = version__mgraph_ai_service_cache
        return self

    def setup_routes(self):
        self.add_routes(Routes__Store     )
        self.add_routes(Routes__Retrieve  )
        self.add_routes(Routes__Exists    )
        self.add_routes(Routes__Delete    )
        self.add_routes(Routes__Namespace )
        self.add_routes(Routes__Server    )
        self.add_routes(Routes__Info      )
        self.add_routes(Routes__Set_Cookie)
        self.add_routes(Routes__Cache     )         # to remove one all methods have been refactored out

    def setup_middlewares(self):
        super().setup_middlewares()
        self.app().add_middleware(BodyReaderMiddleware)
        return self


# intercept the bytes submitted here

# todo: refactor this into a separate class (and maybe even to the Fast_API class), once we confirm that it works ok
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
class BodyReaderMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method in ["POST", "PUT", "PATCH"]:                              # Only read body for certain methods
            body = await request.body()
            request.state.body = body                                               # Store it in request state for later sync access
        else:
            request.state.body = None

        response = await call_next(request)
        return response


