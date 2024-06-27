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

import os
import sys

from dateutil.tz import tzlocal, tzwinlocal

raw_input = input


def compat_input(prompt, interactive_long_input=False):
    """
    Cygwin's pty's are based on pipes. Therefore, when it interacts with a Win32
    program (such as Win32 python), what that program sees is a pipe instead of
    a console. This is important because python buffers pipes, and so on a
    pty-based terminal, text will not necessarily appear immediately. In most
    cases, this isn't a big deal. But when we're doing an interactive prompt,
    the result is that the prompts won't display until we fill the buffer. Since
    raw_input does not flush the prompt, we need to manually write and flush it.

    See https://github.com/mintty/mintty/issues/56 for more details.
    """
    is_windows = sys.platform.startswith('win')
    if interactive_long_input:
        # See THUN-222 for context on why this is necessary
        if is_windows is False:
            os.system('stty -icanon')
    sys.stdout.write(prompt)
    try:
        sys.stdout.flush()
        return raw_input()
    finally:
        if is_windows is False:
            os.system('stty sane')


def compat_tzlocal():
    # https://github.com/dateutil/dateutil/issues/197
    # on windows to avoid this error-
    # return time.localtime(timestamp + time.timezone).tm_isdst
    # ValueError: (22, 'Invalid argument') use
    # tzwinlocal in place of tzlocal
    is_windows = sys.platform.startswith('win')
    if is_windows:
        return tzwinlocal()
    else:
        return tzlocal()
