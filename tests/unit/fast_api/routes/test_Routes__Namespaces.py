from unittest                                                       import TestCase
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespaces     import Routes__Namespaces
from tests.unit.Service__Cache__Test_Objs                           import setup__service__cache__test_objs


class test_Routes__Namespaces(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs         = setup__service__cache__test_objs()
        #cls.cache_fixtures    = cls.test_objs.cache_fixtures
        cls.cache_service     = cls.test_objs.cache_service
        cls.routes_namespaces = Routes__Namespaces(cache_service = cls.cache_service)

    def test__init__(self):
        with self.routes_namespaces as _:
            assert type(_) is Routes__Namespaces

    def test_list(self):
        with self.routes_namespaces as _:
            assert 'fixtures-namespace' in _.list()

