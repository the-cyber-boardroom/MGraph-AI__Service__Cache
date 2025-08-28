from unittest                                       import TestCase
from osbot_aws.aws.lambda_.Lambda                   import Lambda
from osbot_utils.utils.Dev                          import pprint
from osbot_utils.utils.Env                          import load_dotenv
from osbot_utils.utils.Files                        import path_combine, file_exists
from tests.deploy_aws.test_Deploy__Service__base    import test_Deploy__Service__base

class test_Deploy__Service__to__dev(test_Deploy__Service__base, TestCase):
    stage = 'dev'

    # @classmethod
    # def setUpClass(cls):
    #     dot_env = path_combine(__file__, '../.env')
    #     assert file_exists(dot_env) is True
    #     load_dotenv(dot_env)
    #     super().setUpClass()
    #
    # def test_invoke_return_logs(self):
    #     with self.deploy_fast_api.lambda_function() as _:
    #         assert type(_) is Lambda
    #         result  = _.invoke_return_logs()
    #         pprint(result)