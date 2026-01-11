import mgraph_ai_service_cache__web_console
from osbot_fast_api_serverless.deploy.Deploy__Serverless__Fast_API   import Deploy__Serverless__Fast_API
from mgraph_ai_service_cache.config                                  import SERVICE_NAME, LAMBDA_DEPENDENCIES__FAST_API_SERVERLESS
from mgraph_ai_service_cache.fast_api.lambda_handler                 import run

class Deploy__Service(Deploy__Serverless__Fast_API):


    def deploy_lambda(self):
        with super().deploy_lambda() as _:
            _.add_folder(mgraph_ai_service_cache__web_console.path)
            return _

    def handler(self):
        return run

    def lambda_dependencies(self):
        return LAMBDA_DEPENDENCIES__FAST_API_SERVERLESS

    def lambda_name(self):
        return SERVICE_NAME