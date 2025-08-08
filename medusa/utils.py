# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys
import pathlib
import traceback
import tempfile
from datetime import datetime, timedelta, timezone


class MedusaTempFile(object):

    def __init__(self, max_backup_marker_age):
        self._max_backup_marker_age = float(max_backup_marker_age)
        self._tempfile = None
        self._tempfile_path = f'{tempfile.gettempdir()}/medusa_backup_in_progress'

    def _is_stale(self):
        logging.debug(f'max_backup_marker_age: {self._max_backup_marker_age}')
        try:
            path = pathlib.Path(self._tempfile_path)
            file_time = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
            logging.debug(f'marker file creation time: {file_time}')
            return datetime.now(timezone.utc) - file_time > timedelta(hours=self._max_backup_marker_age)
        except Exception as e:
            logging.debug('exception checking for stale marker file: {}'.format(str(e)))
            return False

    def create(self):
        try:
            self._tempfile = open(self._tempfile_path, 'wb')
        except Exception:
            logging.warning(f'Could not create running backup marker at {self._tempfile_path}')

    def delete(self):
        try:
            if self.exists():
                self._tempfile.close()
                pathlib.Path(self._tempfile_path).unlink()
        except Exception:
            pass

    def exists(self):
        try:
            path = pathlib.Path(self._tempfile_path)
            if not path.exists():
                return False
            if self._is_stale():
                logging.warning('Deleting and ignoring stale backup marker')
                self.delete()
                return False
            return True
        except Exception:
            logging.warning(
                f'Could not check for running backup marker {self._tempfile_path}. Assuming a backup is not running'
            )
            return False

    def get_path(self):
        return self._tempfile_path


def evaluate_boolean(value):
    # same behaviour as python's configparser
    if str(value).lower() in ('0', 'false', 'no', 'off'):
        return False
    elif str(value).lower() in ('1', 'true', 'yes', 'on'):
        return True
    else:
        raise TypeError('{} not a boolean'.format(value))


def handle_exception(exception, msg, config):
    if evaluate_boolean(config.grpc.enabled):
        # Propagate the exception when running gRPC server so that exception/error
        # details can be sent back to client.
        raise exception
    else:
        logging.error(msg)
        traceback.print_exc()
        sys.exit(1)


def null_if_empty(value):
    if (value is None):
        return None
    if (str(value) == ''):
        return None
    return value
