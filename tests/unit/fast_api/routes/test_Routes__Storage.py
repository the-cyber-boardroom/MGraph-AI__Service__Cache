from unittest                                                       import TestCase
from mgraph_ai_service_cache.fast_api.routes.Routes__Admin__Storage import Routes__Admin__Storage
from tests.unit.Service__Fast_API__Test_Objs                        import setup__service_fast_api_test_objs


class test_Routes__Admin__Storage(TestCase):

    @classmethod
    def setup_class(cls):
        setup__service_fast_api_test_objs()
        cls.routes_storage = Routes__Admin__Storage()

    def test_folders(self):
        with self.routes_storage as _:
            result = _.folders()

            assert len(result) > 0
