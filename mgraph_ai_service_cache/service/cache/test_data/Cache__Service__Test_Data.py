from typing                                                                                          import List, Dict, Any
from osbot_utils.type_safe.Type_Safe                                                                 import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                      import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now                     import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                    import Safe_Str__File__Path
from osbot_utils.decorators.methods.cache_on_self                                                    import cache_on_self
from osbot_utils.utils.Zip                                                                           import zip_bytes_empty
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy                 import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                       import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Store__Request           import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Store__Request             import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Request         import Schema__Cache__Zip__Operation__Request
from mgraph_ai_service_cache.service.cache.Cache__Service                                            import Cache__Service
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                               import Cache__Service__Store
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store                          import Cache__Service__Data__Store
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store                            import Cache__Service__Zip__Store
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Operations                       import Cache__Service__Zip__Operations
from mgraph_ai_service_cache.schemas.test_data.Schema__Test_Data                                     import Schema__Test_Data__Create__Response
from mgraph_ai_service_cache.schemas.test_data.Schema__Test_Data                                     import Schema__Test_Data__Clear__Response

DEFAULT_NAMESPACES = [ 'test-data', 'test-demo', 'test-analytics'] # 'default',
DEFAULT_STRATEGIES = [ Enum__Cache__Store__Strategy.DIRECT          ,
                       Enum__Cache__Store__Strategy.TEMPORAL        ,
                       Enum__Cache__Store__Strategy.TEMPORAL_LATEST ,
                       Enum__Cache__Store__Strategy.KEY_BASED       ]


class Cache__Service__Test_Data(Type_Safe):                                      # Service for generating test data for the web console
    cache_service : Cache__Service

    @cache_on_self
    def store_service(self) -> Cache__Service__Store:
        return Cache__Service__Store(cache_service=self.cache_service)

    @cache_on_self
    def data_store_service(self) -> Cache__Service__Data__Store:
        return Cache__Service__Data__Store(cache_service=self.cache_service)

    @cache_on_self
    def zip_store_service(self) -> Cache__Service__Zip__Store:
        return Cache__Service__Zip__Store(cache_service=self.cache_service)

    @cache_on_self
    def zip_ops_service(self) -> Cache__Service__Zip__Operations:
        return Cache__Service__Zip__Operations(cache_service=self.cache_service)

    def create_comprehensive(self) -> Schema__Test_Data__Create__Response:       # Create comprehensive test data across all strategies and namespaces
        entries    = []
        namespaces = DEFAULT_NAMESPACES
        strategies = DEFAULT_STRATEGIES

        for namespace in namespaces:
            namespace_entries = self._create_namespace_test_data(namespace, strategies)
            entries.extend(namespace_entries)

        return Schema__Test_Data__Create__Response(success         = True                             ,
                                                   timestamp       = Timestamp_Now()                  ,
                                                   entries_created = len(entries)                     ,
                                                   namespaces      = namespaces                       ,
                                                   strategies_used = [s.value for s in strategies]    ,
                                                   entries         = entries                          ,
                                                   message         = 'Comprehensive test data created')

    def create_for_namespace(self, namespace: Safe_Str__Id                       # Create test data for a specific namespace
                             ) -> Schema__Test_Data__Create__Response:
        strategies = DEFAULT_STRATEGIES
        entries    = self._create_namespace_test_data(str(namespace), strategies)

        return Schema__Test_Data__Create__Response(success         = True                                ,
                                                   timestamp       = Timestamp_Now()                     ,
                                                   entries_created = len(entries)                        ,
                                                   namespaces      = [str(namespace)]                    ,
                                                   strategies_used = [s.value for s in strategies]       ,
                                                   entries         = entries                             ,
                                                   message         = f'Test data created for {namespace}')

    def create_minimal(self) -> Schema__Test_Data__Create__Response:             # Create minimal test data (quick setup)
        namespace = 'default'
        entries   = self._create_json_entries(namespace, Enum__Cache__Store__Strategy.DIRECT, count=3)

        return Schema__Test_Data__Create__Response(success         = True                        ,
                                                   timestamp       = Timestamp_Now()             ,
                                                   entries_created = len(entries)                ,
                                                   namespaces      = [namespace]                 ,
                                                   strategies_used = ['direct']                  ,
                                                   entries         = entries                     ,
                                                   message         = 'Minimal test data created')

    def clear_namespace(self, namespace: Safe_Str__Id                            # Clear all data from a specific namespace
                        ) -> Schema__Test_Data__Clear__Response:
        try:
            storage_fs = self.store_service().cache_service.storage_fs()
            files      = storage_fs.folder__files__all(parent_folder=namespace)
            deleted    = 0

            for file_path in files:
                if storage_fs.file__delete(file_path):
                    deleted += 1

            return Schema__Test_Data__Clear__Response(success       = True                                              ,
                                                      namespace     = namespace                                         ,
                                                      files_deleted = deleted                                           ,
                                                      message       = f'Cleared {deleted} files from {namespace}',
                                                      error         = ''                                        )
        except Exception as e:
            return Schema__Test_Data__Clear__Response(success       = False     ,
                                                      namespace     = namespace ,
                                                      files_deleted = 0         ,
                                                      message       = ''        ,
                                                      error         = str(e)    )

    def _create_namespace_test_data(self, namespace: str,                        # Create all test data for a single namespace
                                    strategies: List[Enum__Cache__Store__Strategy]
                                    ) -> List[Dict]:
        entries = []

        for strategy in strategies:
            json_entries = self._create_json_entries(namespace, strategy)
            entries.extend(json_entries)

            string_entries = self._create_string_entries(namespace, strategy)
            entries.extend(string_entries)

            if strategy == Enum__Cache__Store__Strategy.KEY_BASED:
                key_entries = self._create_key_based_entries(namespace)
                entries.extend(key_entries)

        if namespace == 'default':                                               # Special data for default namespace
            child_entries = self._create_entries_with_child_data(namespace)
            entries.extend(child_entries)

            zip_entries = self._create_zip_entries(namespace)
            entries.extend(zip_entries)

        return entries

    def _create_json_entries(self, namespace: str,                               # Create JSON cache entries
                             strategy: Enum__Cache__Store__Strategy,
                             count: int = 5
                             ) -> List[Dict]:
        entries   = []
        test_data = self._get_sample_json_data()

        for i, data in enumerate(test_data[:count]):
            result = self.store_service().store_json(data      = data      ,
                                                     strategy  = strategy  ,
                                                     namespace = namespace)
            if result:
                entries.append({ "cache_id"    : str(result.cache_id)                      ,
                                 "cache_hash"  : str(result.cache_hash)                    ,
                                 "namespace"   : namespace                                 ,
                                 "strategy"    : strategy.value                            ,
                                 "data_type"   : Enum__Cache__Data_Type.JSON.value         ,
                                 "description" : data.get('description', f'JSON entry {i}')})
        return entries

    def _create_string_entries(self, namespace: str,                             # Create string cache entries
                               strategy: Enum__Cache__Store__Strategy,
                               count: int = 3
                               ) -> List[Dict]:
        entries      = []
        test_strings = [ "Simple text content for cache testing"                                          ,
                         "Configuration: debug=true, log_level=INFO, max_connections=100"                 ,
                         "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor."]

        for i, data in enumerate(test_strings[:count]):
            result = self.store_service().store_string(data      = data      ,
                                                       strategy  = strategy  ,
                                                       namespace = namespace)
            if result:
                entries.append({ "cache_id"    : str(result.cache_id)                ,
                                 "cache_hash"  : str(result.cache_hash)              ,
                                 "namespace"   : namespace                           ,
                                 "strategy"    : strategy.value                      ,
                                 "data_type"   : Enum__Cache__Data_Type.STRING.value ,
                                 "description" : f'String entry {i}'                 })
        return entries

    def _create_key_based_entries(self, namespace: str) -> List[Dict]:           # Create key-based organized entries
        entries        = []
        key_based_data = self._get_key_based_test_data()

        for item in key_based_data:
            result = self.store_service().store_json(data      = item["data"]                             ,
                                                     strategy  = Enum__Cache__Store__Strategy.KEY_BASED   ,
                                                     namespace = namespace                                ,
                                                     cache_key = item["cache_key"]                        ,
                                                     file_id   = item["file_id"]                          )
            if result:
                entries.append({ "cache_id"    : str(result.cache_id)              ,
                                 "cache_hash"  : str(result.cache_hash)            ,
                                 "namespace"   : namespace                         ,
                                 "strategy"    : "key_based"                       ,
                                 "data_type"   : Enum__Cache__Data_Type.JSON.value ,
                                 "cache_key"   : item["cache_key"]                 ,
                                 "description" : f'Key-based: {item["cache_key"]}' })
        return entries

    def _create_entries_with_child_data(self, namespace: str) -> List[Dict]:     # Create parent entries with child data files
        entries     = []
        parent_data = { "type"        : "document"                          ,
                        "title"       : "Analysis Report"                   ,
                        "created"     : "2025-01-09T10:30:00Z"               ,
                        "author"      : "system"                            ,
                        "description" : "Document with child data files"    }

        result = self.store_service().store_json(data      = parent_data                                 ,
                                                 strategy  = Enum__Cache__Store__Strategy.TEMPORAL_LATEST,
                                                 namespace = Safe_Str__Id(namespace)                     )

        if result:
            parent_entry = { "cache_id"    : str(result.cache_id)              ,
                             "cache_hash"  : str(result.cache_hash)            ,
                             "namespace"   : namespace                         ,
                             "strategy"    : "temporal_latest"                 ,
                             "data_type"   : Enum__Cache__Data_Type.JSON.value ,
                             "description" : "Parent with child data"          ,
                             "children"    : []                                }

            child_data_items = self._get_child_data_items()

            for child in child_data_items:
                request = Schema__Cache__Data__Store__Request(cache_id     = result.cache_id                              ,
                                                              data         = child["data"]                                ,
                                                              data_type    = Enum__Cache__Data_Type.JSON                  ,
                                                              data_key     = Safe_Str__File__Path(child["data_key"])      ,
                                                              data_file_id = Safe_Str__Id(child["data_file_id"])          ,
                                                              namespace    = Safe_Str__Id(namespace)                      )
                child_result = self.data_store_service().store_data(request)
                if child_result:
                    parent_entry["children"].append({ "data_key"     : child["data_key"]     ,
                                                      "data_file_id" : child["data_file_id"] })

            entries.append(parent_entry)

        return entries

    def _create_zip_entries(self, namespace: str) -> List[Dict]:                 # Create ZIP package entries
        entries   = []
        zip_bytes = zip_bytes_empty()

        request    = Schema__Cache__Zip__Store__Request(zip_bytes = zip_bytes                                     ,
                                                        cache_key = Safe_Str__File__Path("packages/sample-app")   ,
                                                        file_id   = Safe_Str__Id("v1.0.0")                        ,
                                                        namespace = Safe_Str__Id(namespace)                       ,
                                                        strategy  = Enum__Cache__Store__Strategy.KEY_BASED        )
        zip_result = self.zip_store_service().store_zip(request)

        if zip_result and zip_result.success:
            files_to_add     = self._get_zip_file_contents()
            current_cache_id = zip_result.cache_id

            for file_info in files_to_add:
                add_request = Schema__Cache__Zip__Operation__Request(cache_id     = current_cache_id                         ,
                                                                     operation    = "add"                                    ,
                                                                     file_path    = Safe_Str__File__Path(file_info["path"])  ,
                                                                     file_content = file_info["content"]                     ,
                                                                     namespace    = Safe_Str__Id(namespace)                  )
                add_result = self.zip_ops_service().perform_operation(add_request)
                if add_result and add_result.success and add_result.cache_id:
                    current_cache_id = add_result.cache_id

            entries.append({ "cache_id"    : str(current_cache_id)           ,
                             "cache_hash"  : str(zip_result.cache_hash)      ,
                             "namespace"   : namespace                       ,
                             "strategy"    : "key_based"                     ,
                             "data_type"   : "zip"                           ,
                             "cache_key"   : "packages/sample-app"           ,
                             "description" : "Sample ZIP package with files" })

        return entries

    def _get_sample_json_data(self) -> List[Dict]:                               # Sample JSON data for testing
        return [
            { "type"        : "user_profile"                                      ,
              "user_id"     : "usr_12345"                                         ,
              "email"       : "test@example.com"                                  ,
              "preferences" : { "theme": "dark", "notifications": True }          ,
              "description" : "Sample user profile"                               },

            { "type"        : "api_response"                                      ,
              "endpoint"    : "/api/v1/data"                                      ,
              "status"      : 200                                                 ,
              "data"        : { "items": [1, 2, 3], "total": 3 }                  ,
              "description" : "Cached API response"                               },

            { "type"        : "analytics_event"                                   ,
              "event_name"  : "page_view"                                         ,
              "timestamp"   : "2025-01-09T14:30:00Z"                              ,
              "properties"  : { "page": "/dashboard", "duration_ms": 1500 }       ,
              "description" : "Analytics tracking event"                          },

            { "type"        : "configuration"                                     ,
              "service"     : "cache-service"                                     ,
              "version"     : "0.5.68"                                            ,
              "settings"    : { "max_size": 1024, "ttl": 3600, "compression": True},
              "description" : "Service configuration"                             },

            { "type"        : "workflow_state"                                    ,
              "workflow_id" : "wf_abc123"                                         ,
              "status"      : "completed"                                         ,
              "steps"       : ["init", "process", "validate", "complete"]         ,
              "description" : "Workflow execution state"                          },

            { "type"        : "model_output"                                      ,
              "model"       : "sentiment-analyzer"                                ,
              "input_hash"  : "abc123def456"                                      ,
              "output"      : { "sentiment": "positive", "confidence": 0.92 }     ,
              "description" : "ML model cached output"                            },

            { "type"        : "session_data"                                      ,
              "session_id"  : "sess_xyz789"                                       ,
              "user_id"     : "usr_12345"                                         ,
              "expires_at"  : "2025-01-10T14:30:00Z"                              ,
              "description" : "User session information"                          }
        ]

    def _get_key_based_test_data(self) -> List[Dict]:                            # Key-based hierarchical test data
        return [
            { "cache_key" : "reports/2025/q1/sales"      ,
              "file_id"   : "quarterly-report"           ,
              "data"      : { "quarter"  : "Q1 2025"      ,
                              "revenue"  : 1250000       ,
                              "growth"   : 12.5          ,
                              "regions"  : ["NA", "EMEA"]}},

            { "cache_key" : "reports/2025/q1/users"      ,
              "file_id"   : "user-metrics"               ,
              "data"      : { "quarter"   : "Q1 2025"     ,
                              "new_users" : 15420        ,
                              "active"    : 89340        ,
                              "churn"     : 2.1          }},

            { "cache_key" : "config/services/cache"      ,
              "file_id"   : "cache-config"               ,
              "data"      : { "ttl_hours"    : 24         ,
                              "max_size_mb"  : 1024       ,
                              "compression"  : True       ,
                              "storage_mode" : "s3"       }},

            { "cache_key" : "config/services/api"        ,
              "file_id"   : "api-config"                 ,
              "data"      : { "rate_limit"    : 1000       ,
                              "timeout_ms"    : 30000      ,
                              "retry_count"   : 3          ,
                              "circuit_break" : True       }},

            { "cache_key" : "logs/2025/01/09"            ,
              "file_id"   : "daily-summary"              ,
              "data"      : { "date"           : "2025-01-09",
                              "requests"       : 156780     ,
                              "errors"         : 234        ,
                              "avg_latency_ms" : 45         }}
        ]

    def _get_child_data_items(self) -> List[Dict]:                               # Child data items for parent entries
        return [
            { "data_key"     : "analysis/sentiment"                        ,
              "data_file_id" : "sentiment-v1"                              ,
              "data"         : { "positive" : 0.72                         ,
                                 "negative" : 0.18                         ,
                                 "neutral"  : 0.10                         }},

            { "data_key"     : "analysis/keywords"                         ,
              "data_file_id" : "keywords-v1"                               ,
              "data"         : { "keywords" : ["cache", "performance", "api"],
                                 "scores"   : [0.95, 0.87, 0.82]           }},

            { "data_key"     : "metadata"                                  ,
              "data_file_id" : "file-metadata"                             ,
              "data"         : { "source"    : "import"                    ,
                                 "processed" : True                        ,
                                 "version"   : 1                           }}
        ]

    def _get_zip_file_contents(self) -> List[Dict]:                              # Files to add to ZIP package
        return [
            { "path"    : "README.md"                                                 ,
              "content" : b"# Sample Application\n\nThis is a test package."         },

            { "path"    : "config.json"                                              ,
              "content" : b'{"name": "sample-app", "version": "1.0.0"}'               },

            { "path"    : "src/main.py"                                              ,
              "content" : b'print("Hello from cache!")'                              },

            { "path"    : "src/utils.py"                                             ,
              "content" : b'def helper(): return True'                               }
        ]