import mgraph_ai_service_cache
from unittest                                  import TestCase
from osbot_utils.utils.Files                   import parent_folder, file_name
from mgraph_ai_service_cache.utils.Version      import Version, version__mgraph_ai_service_cache


class test_Version(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.version = Version()

    def test_path_code_root(self):
        assert self.version.path_code_root() == mgraph_ai_service_cache.path

    def test_path_version_file(self):
        with self.version as _:
            assert parent_folder(_.path_version_file()) == mgraph_ai_service_cache.path
            assert file_name    (_.path_version_file()) == 'version'

    def test_value(self):
        assert self.version.value() == version__mgraph_ai_service_cache