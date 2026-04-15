# Copyright 2026 Cloudera, Inc. All rights reserved.
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
import logging
import os
import re
import sys

from cdpcli.arguments import CustomArgument


LOG = logging.getLogger(__name__)

DEFAULT_OUTFILE_NAME = 'file.out'
OUTFILE_ARG_NAME = 'outfile'


def outfile_writer(parsed_args, http, response):
    """
    Write the response body to file, with filename derived from the
    --outfile argument or the response headers content-disposition.
    """
    LOG.debug('Save response body to file')
    filepath = getattr(parsed_args, OUTFILE_ARG_NAME, None)
    if filepath:
        if os.path.isdir(filepath):
            # append the server-provided filename to the user-provided path
            filepath = os.path.join(filepath, _get_filename_from_headers(http))
    else:
        filepath = _get_filename_from_headers(http)

    filepath = _get_available_filename(filepath)
    with open(filepath, 'wb') as f:
        f.write(response)
    sys.stdout.write("Downloaded file saved as '%s'.\n" % filepath)


def _get_available_filename(path):
    """
    If path exists, return path with _N before extension (e.g. file.txt -> file_1.txt).
    Otherwise return path unchanged.
    """
    if not os.path.exists(path):
        return path
    dirname, basename = os.path.split(path)
    name, ext = os.path.splitext(basename)
    n = 1
    while True:
        new_basename = '%s_%d%s' % (name, n, ext)
        new_path = os.path.join(dirname, new_basename)
        if not os.path.exists(new_path):
            return new_path
        n += 1


def _get_filename_from_headers(http):
    """
    Extract the filename from the http response headers.
    """
    content_disposition = http.headers.get('content-disposition', None)
    if content_disposition:
        # ultimately, this is insufficient at handling all edge cases in the
        # filename. However, as long as the server does not pass any 'odd'
        # filenames it should be servicable.
        match = re.search(r'filename\s*=\s*(["\'])?([^"\';]+)(?(1)\1|)',
                          content_disposition, re.I)
        if match and match.group(2):
            return match.group(2)

    return DEFAULT_OUTFILE_NAME


def add_outfile_params(operation_model, argument_table):
    """
    Add the `--outfile` parameter to the command's argument table.
    """
    if not operation_model.has_outfile:
        return

    LOG.debug("Adding outfile parameter for operation: %s" %
              operation_model.name)

    arg = OutfileArgument()
    arg.add_to_arg_table(argument_table)


class OutfileArgument(CustomArgument):
    ARG_DATA = {
        'name': OUTFILE_ARG_NAME,
        'help_text': 'Filename where content will be saved. This overrides the '
                     'filename provided in the response object.',
        'action': 'store',
        'required': False,
        'cli_type_name': 'string'}

    def __init__(self):
        super(OutfileArgument, self).__init__(**self.ARG_DATA)
