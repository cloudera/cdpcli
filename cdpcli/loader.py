# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2016 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import glob
import hashlib
import os
import pickle

from cdpcli import CDPCLI_ROOT
from cdpcli.compat import json
from cdpcli.compat import OrderedDict
from cdpcli.exceptions import DataNotFoundError
from cdpcli.exceptions import ValidationError
import yaml

DEFAULT_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cdp", "model-cache")


def instance_cache(func):
    """Cache the result of a method on a per instance basis.

    This is not a general purpose caching decorator. In order for this to be used,
    it must be used on methods on an instance, and that instance *must* provide a
    ``self._cache`` dictionary.

    """
    def _wrapper(self, *args, **kwargs):
        key = (func.__name__,) + args
        for pair in sorted(kwargs.items()):
            key += pair
        if key in self._cache:
            return self._cache[key]
        data = func(self, *args, **kwargs)
        self._cache[key] = data
        return data
    return _wrapper


class JSONFileLoader(object):
    def exists(self, file_path):
        return os.path.isfile(file_path)

    def load_file(self, file_path):
        if not os.path.isfile(file_path):
            return

        # By default the file will be opened with locale encoding on Python 3.
        # We specify "utf8" here to ensure the correct behavior.
        with open(file_path, 'rb') as fp:
            payload = fp.read().decode('utf-8')
            return json.loads(payload, object_pairs_hook=OrderedDict)


class YAMLFileLoader(object):
    SERVICE_ALIAS_KEY = 'x-service-alias'
    CACHE_BASE_DIR = os.environ.get("CDP_MODEL_CACHE_DIR", DEFAULT_CACHE_DIR)

    def exists(self, file_path):
        return os.path.isfile(file_path)

    def load_file(self, file_path):
        if not os.path.isfile(file_path):
            return
        # By default the file will be opened with locale encoding on Python 3.
        # We open the file in binary mode and then decode it specifically as 'utf8'
        # below to ensure the correct behavior.
        with open(file_path, 'rb') as fp:
            payload_bytes = fp.read()
        return self._pickle_cache(
            hashlib.sha1(payload_bytes).hexdigest(),
            lambda: self._ordered_load(payload_bytes.decode('utf-8')))

    def _pickle_cache(self, cache_key, func):
        """
        Cache the results of 'func' by storing the result pickled on disk.
        'cache_key' should be a hash that uniquely identifies the invocation.
        """
        cache_path = os.path.join(self.CACHE_BASE_DIR, "%s.pickle" % cache_key)
        try:
            with open(cache_path, "rb") as cache:
                try:
                    return pickle.load(cache)
                except Exception:
                    # If we have a deserialization error, remove the corrupt
                    # cache.
                    os.unlink(cache_path)
        except Exception:
            # Ignore failures to read the cache (file may not exist, etc).
            pass
        ret = func()
        # Try to write back to the cache.
        try:
            if not os.path.exists(self.CACHE_BASE_DIR):
                os.makedirs(self.CACHE_BASE_DIR)
            tmp_path = cache_path + ".tmp"
            with open(tmp_path, "wb") as cache:
                pickle.dump(ret, cache)
            os.rename(tmp_path, cache_path)
        except Exception:
            # Failure to write the cache isn't fatal (maybe we are on a read-only
            # filesystem, etc).
            pass
        return ret

    def load_alias(self, file_path):
        data = self.load_file(file_path)
        if data is not None and YAMLFileLoader.SERVICE_ALIAS_KEY in data:
            return data[YAMLFileLoader.SERVICE_ALIAS_KEY]

    def _ordered_load(self, stream):
        class OrderedLoader(yaml.SafeLoader):
            pass

        def construct_mapping(loader, node):
            loader.flatten_mapping(node)
            return OrderedDict(loader.construct_pairs(node))

        OrderedLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            construct_mapping)
        return yaml.load(stream, OrderedLoader)


class Loader(object):
    """Find and load data models.

    This class will handle searching for and loading data models.

    """

    # The included models in cdpcli/data/ that we ship with cdpcli
    BUILTIN_DATA_PATH = os.path.join(CDPCLI_ROOT, 'data')
    # For convenience we automatically add ~/.cdp/models to the data path.
    CUSTOMER_DATA_PATH = os.path.join(os.path.expanduser('~'), '.cdp', 'models')

    def __init__(self):
        self._cache = {}
        # Note: the order of search paths determines the YAML file in case if file
        # names are the same (the first one wins). See self._service_directory method.
        self._search_paths = [self.CUSTOMER_DATA_PATH, self.BUILTIN_DATA_PATH]
        self._directory, self._aliases = self._service_directory()

    # returns a list of actual existing directories among search paths
    def _potential_locations(self):
        for path in self._search_paths:
            if os.path.isdir(path):
                yield path

    # lists potential service names in a given search path
    # which is obtained by sallow listing of subdirectories
    def _potential_services(self, path):
        for child in os.listdir(path):
            full_path = os.path.join(path, child)
            if os.path.isdir(full_path):
                yield (child, full_path)

    def _load_builtin_aliases(self):
        """Load aliases of built-in services generated at build time"""
        alias_path = os.path.join(self.BUILTIN_DATA_PATH, "aliases.yaml")
        with open(alias_path, 'r') as r:
            return yaml.safe_load(r)

    # build a 1:1 map of services to their YAML definition files
    # if multiple definitions are found in search path we will use one of them
    def _service_directory(self):
        dir = {}
        aliases = set()
        for possible_path in self._potential_locations():
            for (service_name, service_path) in self._potential_services(possible_path):
                for file in glob.glob(os.path.join(service_path, "*.yaml")):
                    # accumulate the last path in ascending lexical order,
                    # by filename (not full path) across all search paths
                    if service_name not in dir:
                        dir[service_name] = file
                    elif os.path.split(dir[service_name])[-1] != os.path.split(file)[-1]:
                        dir[service_name] = max(dir[service_name], file,
                                                key=lambda x: os.path.split(x)[-1])
        # Determine service aliases.
        # We expect all built-in services to have a built-in alias.
        # Customer-provided services are parsed to determine aliases.
        builtin_aliases = self._load_builtin_aliases()
        for service_name in list(dir.keys()):
            alias = None
            service_fullpath = dir[service_name]
            if service_fullpath.startswith(self.BUILTIN_DATA_PATH):
                if service_name not in builtin_aliases:
                    raise Exception("Could not find alias for service " + service_name)
                alias = builtin_aliases[service_name]
            else:
                alias = YAMLFileLoader().load_alias(dir[service_name])
            if alias is not None:
                dir[alias] = dir[service_name]
                aliases.add(alias)
        return dir, aliases

    # load the first json config file from one of search locations
    def load_json(self, name):
        loader = JSONFileLoader()
        for possible_path in self._potential_locations():
            full_path = os.path.join(possible_path, name)
            found = loader.load_file(full_path)
            if found is not None:
                return found
        raise DataNotFoundError(data_path=name)

    # returns a list of service names in directory
    def list_available_services(self):
        services = self._directory.keys()
        return sorted(services)

    # load YAML data by service name
    @instance_cache
    def load_service_data(self, service_name):
        if service_name not in self._directory.keys():
            raise ValidationError(value=service_name,
                                  param='service_name',
                                  type_name='str')
        return YAMLFileLoader().load_file(self._directory[service_name])

    # wether the input service name is a service alias
    def is_service_alias(self, service_name):
        return service_name in self._aliases
