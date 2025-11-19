from unittest                                                               import TestCase
from memory_fs.storage_fs.providers.Storage_FS__Memory                      import Storage_FS__Memory
from mgraph_ai_service_cache.fast_api.routes.admin.Routes__Admin__Storage   import Routes__Admin__Storage
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API          import CACHE__TEST__FIXTURES__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                   import Cache__Service
from tests.unit.Service__Cache__Test_Objs                                   import setup__service__cache__test_objs

class test_Routes__Admin__Storage(TestCase):

    @classmethod
    def setup_class(cls):
        cls.test_objs      = setup__service__cache__test_objs()
        cls.routes_storage = Routes__Admin__Storage(cache_service=cls.test_objs.cache_service)

    def test__init__(self):
        with self.routes_storage as _:
            assert type(_                           ) is Routes__Admin__Storage
            assert type(_.cache_service             ) is Cache__Service
            assert type(_.cache_service.storage_fs()) is Storage_FS__Memory
            assert _.cache_service                    == self.test_objs.cache_service
            assert _.cache_service                    == self.test_objs.fast_api.cache_service
            assert _.cache_service                    == self.test_objs.cache_fixtures.cache_service
            assert _.cache_service.storage_fs()       == self.test_objs.cache_fixtures.cache_service.storage_fs()


    def test_folders(self):
        with self.routes_storage as _:
            result = _.folders()
            assert len(_.storage_fs().files__paths()) > 10
            assert CACHE__TEST__FIXTURES__NAMESPACE in result

