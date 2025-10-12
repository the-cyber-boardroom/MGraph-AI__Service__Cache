from mgraph_ai_service_cache import package_name

SERVICE_NAME                             = package_name
FAST_API__TITLE                          = "MGraph-AI Service Cache"
FAST_API__DESCRIPTION                    = "Cache Service"
LAMBDA_DEPENDENCIES__FAST_API_SERVERLESS = ['osbot-fast-api-serverless==v1.23.0'    ,
                                            'memory-fs==v0.35.0'                    ,
                                            'mgraph-ai-service-cache-client==v0.5.0']