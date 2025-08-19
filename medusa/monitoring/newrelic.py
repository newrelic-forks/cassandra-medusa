# -*- coding: utf-8 -*-
# Copyright 2021-present Shopify. All rights reserved.
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

import gzip
import json
import logging
import requests

import medusa.utils
from medusa.monitoring.abstract import AbstractMonitoring


class NewRelicMonitoring(AbstractMonitoring):

    def __init__(self, config):
        super().__init__(config)
        self.license_key = self.config.newrelic_license_key
        self.events_url = self.config.newrelic_events_url

        if not self.license_key:
            raise ValueError("newrelic_license_key is required for New Relic monitoring")
        if not self.events_url:
            raise ValueError("newrelic_events_url is required for New Relic monitoring")

    def send(self, tags, value):
        if len(tags) != 3:
            raise AssertionError("New Relic monitoring implementation needs 3 tags: 'name', 'what' and 'backup_name'")

        name, what, backup_name = tags

        # Create event payload
        event = {
            "eventType": "MedusaBackup",
            "medusaEvent": name,
            what: value
        }

        # The backup_name would be a rather high cardinality metrics series if backups are at all frequent.
        # This could be an expensive metric so backup_name is dropped from the tags sent by default
        if medusa.utils.evaluate_boolean(self.config.send_backup_name_tag):
            event["backup_name"] = backup_name

        self._send_event([event])

    def _send_event(self, events):
        """Send events to New Relic Events API"""
        headers = {
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Api-Key": self.license_key,
        }

        # Gzip encode the JSON payload
        json_data = json.dumps(events).encode('utf-8')
        compressed_data = gzip.compress(json_data)

        try:
            response = requests.post(
                self.events_url,
                headers=headers,
                data=compressed_data,
                timeout=30
            )
            response.raise_for_status()
            logging.debug(f"Successfully sent {len(events)} events to New Relic")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send events to New Relic: {e}")
            raise
