from mgraph_ai_service_cache import package_name

SERVICE_NAME                             = package_name
FAST_API__TITLE                          = "MGraph-AI Service Cache"
FAST_API__DESCRIPTION                    = "Cache Service"
LAMBDA_DEPENDENCIES__FAST_API_SERVERLESS = ['memory-fs==v0.40.0'                     ,
                                            'mgraph-ai-service-cache-client==v0.27.0',
                                            'osbot-fast-api-serverless==1.33.0'      ]

CACHE_SERVICE__WEB_CONSOLE__PATH              = 'console'
CACHE_SERVICE__WEB_CONSOLE__ROUTE__START_PAGE = 'index'
CACHE_SERVICE__WEB_CONSOLE__MAJOR__VERSION    = "v0/v0.1"
CACHE_SERVICE__WEB_CONSOLE__LATEST__VERSION   = "v0.1.1"
ROUTES_PATHS__WEB_CONSOLE                     = [f'/{CACHE_SERVICE__WEB_CONSOLE__PATH}']