from mgraph_ai_service_cache import package_name

SERVICE_NAME                             = package_name
FAST_API__TITLE                          = "MGraph-AI Service Cache"
FAST_API__DESCRIPTION                    = "Cache Service"
LAMBDA_DEPENDENCIES__FAST_API_SERVERLESS = ['osbot-fast-api-serverless==1.30.0'    ,
                                            'memory-fs==v0.37.0'                    ,
                                            'mgraph-ai-service-cache-client==v0.22.0']