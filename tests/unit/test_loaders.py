# Copyright (c) 2017 Cloudera, Inc. All rights reserved.

import os
import shutil
import tempfile

from cdpcli.exceptions import DataNotFoundError
from cdpcli.exceptions import ValidationError
from cdpcli.loader import JSONFileLoader
from cdpcli.loader import Loader
from cdpcli.loader import YAMLFileLoader
from mock import patch
from nose.tools import raises
from tests import unittest


LOADER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'loader')
TEST_TMP_DIR = tempfile.mkdtemp("foo.XXXX")


# Base class with common cases across loaders. Not an actual test. Shouldn't run.
class TestFileLoader(unittest.TestCase):
    def test_load_file(self):
        data = self.file_loader.load_file(self.valid_file_path)
        self.assertIsNotNone(data)
        self.assertEqual(len(data), 4)
        self.assertTrue('test_key_1' in data)

    def test_load_file_does_not_exist_returns_none(self):
        # None is used to indicate that the loader could not find a
        # file to load.
        self.assertIsNone(self.file_loader.load_file(self.not_found_path))

    def test_file_exists_check(self):
        self.assertTrue(self.file_loader.exists(self.valid_file_path))

    def test_file_does_not_exist_returns_false(self):
        self.assertFalse(self.file_loader.exists(self.not_found_path))

    def test_file_with_non_ascii(self):
        try:
            self.assertIsNotNone(self.file_loader.load_file(self.non_ascii_file_path))
        except UnicodeDecodeError:
            self.fail('Fail to handle data file with non-ascii characters')


class TestJSONFileLoader(TestFileLoader):
    def setUp(self):
        super(TestJSONFileLoader, self).setUp()
        self.file_loader = JSONFileLoader()
        self.data_path = LOADER_DIR
        self.valid_file_path = os.path.join(self.data_path, 'foo.json')
        self.non_ascii_file_path = os.path.join(self.data_path, 'non_ascii.json')
        self.not_found_path = os.path.join(self.data_path, 'does', 'not', 'exist')


class TestYAMLFileLoader(TestFileLoader):
    def setUp(self):
        super(TestYAMLFileLoader, self).setUp()
        self.file_loader = YAMLFileLoader()
        self.file_loader.CACHE_BASE_DIR = TEST_TMP_DIR
        self.data_path = LOADER_DIR
        self.valid_file_path = os.path.join(self.data_path, 'foo.yaml')
        self.non_ascii_file_path = os.path.join(self.data_path, 'non_ascii.yaml')
        self.not_found_path = os.path.join(self.data_path, 'does', 'not', 'exist')

    def tearDown(self):
        if os.path.exists(TEST_TMP_DIR):
            shutil.rmtree(TEST_TMP_DIR)

    def test_load_alias_does_not_exist_returns_none(self):
        self.assertIsNone(self.file_loader.load_alias(self.not_found_path))

    def test_load_alias_returns_alias(self):
        self.assertEqual(self.file_loader.load_alias(self.valid_file_path), 'bar')


# helper class encapsulating fake methods for Loader tests
class FakeFileSystem(object):
    @staticmethod
    def builtin_full_path(service, file):
        return os.path.join(Loader.BUILTIN_DATA_PATH, service, file)

    @staticmethod
    def customer_full_path(service, file):
        return os.path.join(Loader.CUSTOMER_DATA_PATH, service, file)

    # fake directories in search paths
    @staticmethod
    def listdir(path):
        return {
            Loader.BUILTIN_DATA_PATH: ['service-1', 'service-2', 'service-3', 'service-4',
                                       'service-6'],
            Loader.CUSTOMER_DATA_PATH: ['service-2', 'service-3', 'service-4',
                                        'service-5']
        }[path]

    @staticmethod
    def load_builtin_aliases():
        return {
            'service-1': 'alias-1',
            'service-2': None,
            'service-3': None,
            'service-4': None,
            'service-6': None,
        }

    # fake file layout to test various model selection behaviors
    @staticmethod
    def glob(path):
        # local aliases for brevity
        builtin = FakeFileSystem.builtin_full_path
        customer = FakeFileSystem.customer_full_path

        return {
            # built-in search path
            builtin('service-1', '*.yaml'): [builtin('service-1', 'service-1-1.yaml'),
                                             builtin('service-1', 'service-1-2.yaml')],
            builtin('service-2', '*.yaml'): [builtin('service-2', 'service-2-1.yaml')],
            builtin('service-3', '*.yaml'): [builtin('service-3', 'service-3-1.yaml')],
            builtin('service-4', '*.yaml'): [builtin('service-4', 'service-4-2.yaml')],
            builtin('service-6', '*.yaml'): [],

            # customer home dir search path
            customer('service-2', '*.yaml'): [customer('service-2', 'service-2-1.yaml')],
            customer('service-3', '*.yaml'): [customer('service-3', 'service-3-1.yaml'),
                                              customer('service-3', 'service-3-2.yaml')],
            customer('service-4', '*.yaml'): [],
            customer('service-5', '*.yaml'): [customer('service-5', 'service-5-1.yaml')]
        }[path]


class TestLoader(unittest.TestCase):
    def setUp(self):
        super(TestLoader, self).setUp()

    @patch('glob.glob', side_effect=FakeFileSystem.glob)
    @patch('os.listdir', side_effect=FakeFileSystem.listdir)
    @patch('os.path.isdir', return_value=True)
    @patch('cdpcli.loader.Loader._load_builtin_aliases',
           side_effect=FakeFileSystem.load_builtin_aliases)
    def test_list_available_services(self, *args):
        loader = Loader()
        services = loader.list_available_services()

        self.assertEqual(6, len(services))
        self.assertEqual(len(set(services)), len(services))
        self.assertEqual(0, len(set(services).symmetric_difference(['alias-1',
                                                                    'service-1',
                                                                    'service-2',
                                                                    'service-3',
                                                                    'service-4',
                                                                    'service-5'])))

    @raises(Exception)
    @patch('glob.glob', side_effect=FakeFileSystem.glob)
    @patch('os.listdir', side_effect=FakeFileSystem.listdir)
    @patch('os.path.isdir', return_value=True)
    @patch('cdpcli.loader.Loader._load_builtin_aliases',
           return_value={
            "service-2": None,
            "service-3": None,
            "service-4": None,
            "service-6": None})
    def test_missing_builtin_service_alias(self, *args):
        '''Throws an exception because built-in service-1 is missing'''
        Loader()

    @patch('glob.glob', side_effect=FakeFileSystem.glob)
    @patch('os.listdir', side_effect=FakeFileSystem.listdir)
    @patch('os.path.isdir', return_value=True)
    @patch('cdpcli.loader.Loader._load_builtin_aliases',
           side_effect=FakeFileSystem.load_builtin_aliases)
    def test_load_service_data(self, *args):
        # local aliases for brevity
        builtin = FakeFileSystem.builtin_full_path
        customer = FakeFileSystem.customer_full_path

        correct = [['service-1', builtin('service-1', 'service-1-2.yaml')],
                   ['service-4', builtin('service-4', 'service-4-2.yaml')],
                   ['service-2', customer('service-2', 'service-2-1.yaml')],
                   ['service-3', customer('service-3', 'service-3-2.yaml')],
                   ['service-5', customer('service-5', 'service-5-1.yaml')],
                   ['alias-1', builtin('service-1', 'service-1-2.yaml')]]

        with patch.object(YAMLFileLoader, 'load_alias') as mock_load_alias:
            mock_load_alias.side_effect = \
                lambda path: 'alias-1' if 'service-1' in path else None
            loader = Loader()
            for service in correct:
                with patch.object(YAMLFileLoader, 'load_file') as mock_load_file:
                    mock_load_file.side_effect = lambda path: path
                    self.assertEqual(service[1], loader.load_service_data(service[0]))
                    self.assertEqual(1, mock_load_file.call_count)

        for not_found in ['service-6', 'service-foo']:
            with patch.object(YAMLFileLoader, 'load_file') as mock_load_file:
                with self.assertRaises(ValidationError):
                    loader.load_service_data(not_found)
                self.assertEqual(0, mock_load_file.call_count)

    # ensures that the results of load_service_data is repeatable and that
    # result of the loader is cached
    @patch('glob.glob', side_effect=FakeFileSystem.glob)
    @patch('os.listdir', side_effect=FakeFileSystem.listdir)
    @patch('os.path.isdir', return_value=True)
    @patch('cdpcli.loader.Loader._load_builtin_aliases',
           side_effect=FakeFileSystem.load_builtin_aliases)
    def test_load_service_data_repeatable_cached(self, *args):
        with patch.object(YAMLFileLoader, 'load_file'):
            loader = Loader()
        # warm up with both args and kwargs, ensure results are identical
        with patch.object(YAMLFileLoader, 'load_file') as mock_load_file:
            mock_load_file.side_effect = lambda path: path
            data = loader.load_service_data('service-1')
            self.assertEqual(data, loader.load_service_data(service_name='service-1'))
            self.assertEqual(2, mock_load_file.call_count)
        # ensure results are repeatable and are coming from cache (not calls to load_file)
        with patch.object(YAMLFileLoader, 'load_file') as mock_load_file:
            mock_load_file.side_effect = lambda path: path
            self.assertEqual(data, loader.load_service_data('service-1'))
            self.assertEqual(data, loader.load_service_data(service_name='service-1'))
            self.assertEqual(0, mock_load_file.call_count)

    # loading JSON from search paths by filename
    def test_load_json(self, *args):
        cases = [
            [lambda path: path == Loader.BUILTIN_DATA_PATH, Loader.BUILTIN_DATA_PATH,
             "Loader should use builtin data path."],
            [lambda path: path == Loader.CUSTOMER_DATA_PATH, Loader.CUSTOMER_DATA_PATH,
             "Loader should use customer data path."],
            [lambda path: True, Loader.CUSTOMER_DATA_PATH,
             "Loader should prefer file at customer data path over built-in path."]
        ]

        for case in cases:
            loader = Loader()
            with patch('os.path.isdir') as mock_isdir:
                mock_isdir.side_effect = case[0]
                with patch.object(JSONFileLoader, 'load_file') as mock_load_file:
                    mock_load_file.side_effect = lambda path: path
                    data = loader.load_json('foo.json')
                    self.assertEquals(1, mock_load_file.call_count)
                    self.assertEquals(os.path.join(case[1], 'foo.json'), data, case[2])

    # no data dirs, load JSON
    @patch('os.path.isdir', return_value=False)
    def test_load_json_no_dirs(self, *args):
        with patch.object(JSONFileLoader, 'load_file') as mock_load_file:
            loader = Loader()
            with self.assertRaises(DataNotFoundError):
                loader.load_json('foo.json')
            self.assertEquals(0, mock_load_file.call_count)

    # no data dirs, load service model
    @patch('os.path.isdir', return_value=False)
    def test_load_service_data_no_dirs(self, *args):
        with patch.object(YAMLFileLoader, 'load_file') as mock_load_file:
            loader = Loader()
            with self.assertRaises(ValidationError):
                loader.load_service_data('service-1')
            self.assertEquals(0, mock_load_file.call_count)


# Remove (undefine) base class so that unit test runner doesn't attempt to execute.
# This skips the test without noise in output, unlike when using @unittest.skip decorator.
del TestFileLoader

if __name__ == '__main__':
    unittest.main()
