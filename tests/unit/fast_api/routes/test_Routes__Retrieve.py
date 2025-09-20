import pytest
import json
import base64
from unittest                                                                             import TestCase
from fastapi                                                                              import HTTPException, Response
from osbot_fast_api.api.routes.Fast_API__Routes                                           import Fast_API__Routes
from osbot_utils.testing.__                                                               import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                      import Type_Safe
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash  import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                     import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id           import Safe_Str__Id
from osbot_utils.utils.Misc                                                               import list_set
from osbot_utils.utils.Objects                                                            import base_classes, obj
from mgraph_ai_service_cache.fast_api.routes.Routes__Retrieve                             import Routes__Retrieve, TAG__ROUTES_RETRIEVE
from mgraph_ai_service_cache.service.cache.Service__Cache__Retrieve                       import Service__Cache__Retrieve
from mgraph_ai_service_cache.service.cache.Service__Cache__Store                          import Service__Cache__Store
from mgraph_ai_service_cache.service.cache.Cache__Service                                 import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Binary__Reference               import Schema__Cache__Binary__Reference
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Exists__Response                import Schema__Cache__Exists__Response
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                   import Enum__Cache__Data_Type
from tests.unit.Service__Fast_API__Test_Objs                                              import setup__service_fast_api_test_objs

class test_Routes__Retrieve(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service_fast_api_test_objs()                  # Reuse shared test objects
        cls.cache_fixtures     = cls.test_objs.cache_fixtures                         # Use shared fixtures
        cls.fixtures_bucket    = cls.cache_fixtures.fixtures_bucket                   # Use fixtures bucket
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        
        # Services using fixtures bucket
        cls.cache_service      = Cache__Service          (default_bucket   = cls.fixtures_bucket)
        cls.retrieve_service   = Service__Cache__Retrieve(cache_service    = cls.cache_service  )
        cls.store_service      = Service__Cache__Store   (cache_service    = cls.cache_service  )
        cls.routes             = Routes__Retrieve        (retrieve_service = cls.retrieve_service)
        
        # Test namespace separate from fixtures
        cls.test_namespace     = Safe_Str__Id("test-routes-retrieve")
        
        # Reuse fixture data
        cls.test_string        = cls.cache_fixtures.get_fixture_data('string_simple')
        cls.test_json          = cls.cache_fixtures.get_fixture_data('json_simple')
        cls.test_binary        = cls.cache_fixtures.get_fixture_data('binary_small')
        
        # Pre-existing fixture IDs and hashes
        cls.fixture_id_string  = cls.cache_fixtures.get_fixture_id('string_simple')
        cls.fixture_id_json    = cls.cache_fixtures.get_fixture_id('json_simple')
        cls.fixture_id_binary  = cls.cache_fixtures.get_fixture_id('binary_small')
        
        cls.fixture_hash_string = cls.cache_fixtures.get_fixture_hash('string_simple')
        cls.fixture_hash_json   = cls.cache_fixtures.get_fixture_hash('json_simple')
        cls.fixture_hash_binary = cls.cache_fixtures.get_fixture_hash('binary_small')
        
        # Track created items for cleanup
        cls.created_cache_ids  = []

    @classmethod
    def tearDownClass(cls):                                                          # Clean up only what we created
        for cache_id in cls.created_cache_ids:
            try:
                cls.cache_service.delete_by_id(cache_id, cls.test_namespace)
            except:
                pass

    def _track_and_store(self, data, data_type="string"):                            # Helper to store and track
        if data_type == "string":
            result = self.store_service.store_string(data = data, namespace = self.test_namespace)
        elif data_type == "json":
            result = self.store_service.store_json(data = data, namespace = self.test_namespace)
        elif data_type == "binary":
            result = self.store_service.store_binary(data = data, namespace = self.test_namespace)
        
        if result and result.cache_id not in self.created_cache_ids:
            self.created_cache_ids.append(result.cache_id)
        
        return result

    def test__init__(self):                                                           # Test auto-initialization
        with Routes__Retrieve() as _:
            assert type(_)                   is Routes__Retrieve
            assert base_classes(_)           == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                     == TAG__ROUTES_RETRIEVE
            assert _.prefix                  == '/{namespace}'
            assert type(_.retrieve_service) is Service__Cache__Retrieve

    def test_retrieve__cache_id__not_found(self):                                    # Test 404 for non-existent
        with self.routes as _:
            non_existent_id = Random_Guid()
            
            with pytest.raises(HTTPException) as exc_info:
                _.retrieve__cache_id(non_existent_id, self.test_namespace)
            
            assert exc_info.value.status_code == 404
            error_detail = exc_info.value.detail
            assert error_detail['cache_hash'   ] is None
            assert error_detail['cache_id'     ] == non_existent_id
            assert error_detail['error_type'   ] == 'NOT_FOUND'
            assert error_detail['resource_type'] == 'cache_entry'
            assert list_set(error_detail)        == ['cache_hash' , 'cache_id'     , 'error_type' ,
                                                     'message'    , 'namespace'    , 'request_id' ,
                                                     'resource_id', 'resource_type', 'timestamp'  ]

            assert obj(error_detail) == __(resource_id = None                   ,
                                           cache_hash  = None                   ,
                                           cache_id    = non_existent_id        ,
                                           namespace   = 'test-routes-retrieve' ,
                                           error_type  = 'NOT_FOUND'            ,
                                           message     = 'The requested cache entry was not found',
                                           timestamp   = __SKIP__               ,
                                           request_id  = __SKIP__               ,
                                           resource_type='cache_entry'          )

    def test_retrieve__cache_id__using_fixtures(self):                               # Use existing fixtures
        with self.routes as _:
            # Retrieve existing fixture
            result = _.retrieve__cache_id(self.fixture_id_string, self.fixtures_namespace)
            
            assert result.data              == self.test_string
            assert result.data_type         == Enum__Cache__Data_Type.STRING
            assert result.metadata.cache_id == self.fixture_id_string

    def test_retrieve__cache_id__binary_redirect(self):                              # Test binary redirect with fixture
        with self.routes as _:
            result = _.retrieve__cache_id(self.fixture_id_binary, self.fixtures_namespace)                      # Use existing binary fixture
            
            assert type(result)      is Schema__Cache__Binary__Reference
            assert result.message    == "Binary data requires separate endpoint"
            assert result.data_type  == Enum__Cache__Data_Type.BINARY
            assert result.size       == 0                                                                       # BUG this should be the size of the file
            assert result.binary_url == f"/{self.fixtures_namespace}/retrieve/{self.fixture_id_binary}/binary"

    def test_retrieve__hash__cache_hash__using_fixtures(self):
        with self.routes as _:
            result = _.retrieve__hash__cache_hash(self.fixture_hash_string, self.fixtures_namespace)            # Retrieve by existing fixture hash
            
            assert result.data      == self.test_string
            assert result.data_type == Enum__Cache__Data_Type.STRING

    def test_retrieve__cache_id__string(self):                                       # Test string format with fixture
        with self.routes as _:
            result = _.retrieve__cache_id__string(self.fixture_id_string, self.fixtures_namespace)
            
            assert type(result)      is Response
            assert result.body       == self.test_string.encode()
            assert result.media_type == "text/plain"

    def test_retrieve__cache_id__json(self):                                         # Test JSON format with fixture
        with self.routes as _:
            result = _.retrieve__cache_id__json(self.fixture_id_json, self.fixtures_namespace)
            
            assert result == self.test_json

    def test_retrieve__cache_id__json__invalid(self):                                # Test invalid JSON
        with self.routes as _:
            result = self._track_and_store("not json data")                         # Store non-JSON string in test namespace

            with pytest.raises(HTTPException) as exc_info:
                _.retrieve__cache_id__json(result.cache_id, self.test_namespace)

            assert exc_info.value.status_code == 415
            assert "string, not JSON" in exc_info.value.detail

    def test_retrieve__cache_id__binary(self):                                       # Test binary with fixture
        with self.routes as _:
            result = _.retrieve__cache_id__binary(self.fixture_id_binary, self.fixtures_namespace)
            
            assert type(result)      is Response
            assert result.body       == self.test_binary
            assert result.media_type == "application/octet-stream"

    def test_retrieve__hash__cache_hash__string(self):                               # Test string by hash
        with self.routes as _:
            result = _.retrieve__hash__cache_hash__string(self.fixture_hash_string, 
                                                          self.fixtures_namespace)
            
            assert result.body       == self.test_string.encode()
            assert result.media_type == "text/plain"

    def test_retrieve__hash__cache_hash__json(self):                                 # Test JSON by hash
        with self.routes as _:
            result = _.retrieve__hash__cache_hash__json(self.fixture_hash_json, 
                                                        self.fixtures_namespace)
            
            assert result == self.test_json

    def test_retrieve__hash__cache_hash__binary(self):                               # Test binary by hash
        with self.routes as _:
            result = _.retrieve__hash__cache_hash__binary(self.fixture_hash_binary,
                                                          self.fixtures_namespace)
            
            assert result.body       == self.test_binary
            assert result.media_type == "application/octet-stream"

    def test_retrieve__details__cache_id(self):                                      # Test details with fixture
        with self.routes as _:
            result = _.retrieve__details__cache_id(self.fixture_id_string, 
                                                   self.fixtures_namespace)
            
            assert result.cache_id   == self.fixture_id_string
            assert result.cache_hash == self.fixture_hash_string
            assert result.namespace  == str(self.fixtures_namespace)
            assert result.obj().contains(__(cache_id   = self.fixture_id_string,
                                           cache_hash = self.fixture_hash_string))

    def test_retrieve__exists__cache_hash(self):                                     # Test exists with fixture
        with self.routes as _:
            # Check fixture exists
            result = _.retrieve__exists__cache_hash(self.fixture_hash_string, 
                                                    self.fixtures_namespace)
            
            assert type(result)      is Schema__Cache__Exists__Response
            assert result.exists     is True
            assert result.cache_hash == self.fixture_hash_string
            assert result.namespace  == self.fixtures_namespace

    def test_retrieve__exists__cache_hash__not_found(self):                          # Test non-existent
        with self.routes as _:
            non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")
            
            result = _.retrieve__exists__cache_hash(non_existent_hash, self.test_namespace)
            
            assert result.exists is False

    def test_cross_type_conversion(self):                                                   # Test type conversions with fixture
        with self.routes as _:
            result_string = _.retrieve__cache_id__string(self.fixture_id_json,              # Retrieve JSON fixture as string
                                                         self.fixtures_namespace)
            parsed = json.loads(result_string.body.decode())
            assert parsed == self.test_json

            result_binary = _.retrieve__cache_id__binary(self.fixture_id_json,              # Retrieve JSON fixture as binary
                                                         self.fixtures_namespace)
            parsed = json.loads(result_binary.body.decode())
            assert parsed == self.test_json

    def test_binary_base64_encoding(self):                                           # Test base64 encoding
        with self.routes as _:
            # Use larger binary fixture that can't be UTF-8 decoded
            binary_medium = self.cache_fixtures.get_fixture_data('binary_medium')
            result        = self._track_and_store(binary_medium, "binary")
            
            # Retrieve as string - should base64 encode
            result = _.retrieve__cache_id__string(result.cache_id, self.test_namespace)
            
            # Verify base64 encoding
            expected = base64.b64encode(binary_medium).decode('utf-8')
            assert result.body == expected.encode()

    def test_json_string_parsing(self):                                              # Test JSON string parsing
        with self.routes as _:
            json_string = '{"valid": "json", "number": 42}'                                 # Store valid JSON as string
            result      = self._track_and_store(json_string)

            result = _.retrieve__cache_id__json(result.cache_id, self.test_namespace)       # Retrieve as JSON - should parse
            
            assert result == {"valid": "json", "number": 42}

    def test_default_namespace(self):                                                       # Test default namespace
        with self.routes as _:
            # Store with None namespace
            result = self.store_service.store_string(data = "default test", 
                                                     namespace = None)
            cache_id = result.cache_id
            self.created_cache_ids.append(cache_id)
            
            # Retrieve with None namespace
            result = _.retrieve__cache_id(cache_id, namespace = None)
            
            assert result.data == "default test"

    def test_retrieve_with_different_fixtures(self):                                 # Test variety of fixtures
        fixtures_to_test = [('string_medium', 'string', self.cache_fixtures.get_fixture_id('string_medium')),
                           ('json_complex'  , 'json'  , self.cache_fixtures.get_fixture_id('json_complex' )),
                           ('binary_large'  , 'binary', self.cache_fixtures.get_fixture_id('binary_large' ))]
        
        with self.routes as _:
            for fixture_name, data_type, fixture_id in fixtures_to_test:
                with self.subTest(fixture=fixture_name):
                    if data_type == 'binary':
                        # Binary returns reference
                        result = _.retrieve__cache_id(fixture_id, self.fixtures_namespace)
                        assert type(result) is Schema__Cache__Binary__Reference
                    else:
                        # String and JSON return data
                        result = _.retrieve__cache_id(fixture_id, self.fixtures_namespace)
                        fixture_data = self.cache_fixtures.get_fixture_data(fixture_name)
                        assert result.data == fixture_data