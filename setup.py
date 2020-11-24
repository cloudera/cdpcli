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

import setup_common
from setuptools import find_packages
from setuptools import setup
import versioneer


release = 'public'

setup(
    name='cdpcli',
    version=versioneer.get_version(),
    description='Cloudera CDP Command Line Interface',
    long_description=setup_common.read_long_description(release),
    long_description_content_type='text/markdown',
    url='https://console.cdp.cloudera.com/',
    license='Apache License 2.0',
    author='Cloudera, Inc.',
    classifiers=setup_common.get_classifiers(release),
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=setup_common.get_requirements(release),
    entry_points=setup_common.get_entry_points(release),
    cmdclass=versioneer.get_cmdclass(),
)
