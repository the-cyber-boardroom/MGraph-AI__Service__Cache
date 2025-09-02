from mgraph_ai_service_cache import package_name

SERVICE_NAME                             = package_name
FAST_API__TITLE                          = "MGraph-AI Service Cache"
FAST_API__DESCRIPTION                    = "Cache Service"
LAMBDA_DEPENDENCIES__FAST_API_SERVERLESS = ['osbot-fast-api-serverless==v1.17.0',
                                            'memory-fs==0.20.0'                 ]