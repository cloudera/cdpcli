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

from codecs import open
from os import path


def read_long_description(release):
    """
    Reads the long description of the package from the project README.md file.
    """
    here = path.abspath(path.dirname(__file__))

    # Get the long description from the README file
    with open(path.join(here, 'README.md'), encoding='utf-8') as f:
        return f.read()


def get_requirements(release):
    """
    Gets the prerequisite requirements for the package.
    """
    return ["python-dateutil>=2.1,<3.0.0",
            "docutils==0.14",
            "pyyaml>=3.11",
            "colorama>=0.2.5,<=0.3.9",
            "asn1crypto>=0.21.1",
            "rsa>=3.4.2",
            "pure25519>=0.0.1",
            "requests>=2.21.0",
            "urllib3>=1.21.1"]


def get_classifiers(release):
    """
    Gets the classifiers for the package.
    """
    if release == 'public':
        classifiers = [
            'Development Status :: 5 - Production/Stable',
        ]
    elif release == 'beta':
        classifiers = [
            'Development Status :: 4 - Beta'
        ]
    else:
        classifiers = [
            'Development Status :: 3 - Alpha'
        ]

    classifiers.extend([
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ])
    return classifiers


def get_entry_points(release):
    """
    Gets the entry points for the package.
    """
    return {
        'console_scripts': [
            'cdp=cdpcli.clidriver:main',
            'cdp_completer=cdpcli.completer:main'
        ],
    }
