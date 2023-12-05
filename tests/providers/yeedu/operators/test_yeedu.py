# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.providers.yeedu.operators.yeedu import YeeduJobRunOperator


class TestYeeduJobRunOperator(unittest.TestCase):
    @patch("providers.yeedu.hooks.yeedu.YeeduHook._api_request")
    @patch("airflow.models.Variable.get")
    def test_execute_successful_job(self, mock_variable_get, mock_requests):
        job_conf_id = "123"
        hostname = "test_host"
        workspace_id = 456
        token = "test_token"

        mock_variable_get.return_value = token

        operator = YeeduJobRunOperator(
            task_id="test_task",
            job_conf_id=job_conf_id,
            hostname=hostname,
            workspace_id=workspace_id,
            token=None,
        )

        # Mocking requests.request
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"job_id": 789, "job_status": "DONE"}
        mock_response.text = "Job completed successfully"
        mock_requests.return_value = mock_response

        result = operator.execute({})
        self.assertEqual(result, "Job completed successfully")

    @patch("providers.yeedu.hooks.yeedu.YeeduHook._api_request")
    @patch("airflow.models.Variable.get")
    def test_execute_fail_job(self, mock_variable_get, mock_requests):
        job_conf_id = "123"
        hostname = "test_host"
        workspace_id = 456
        token = "test_token"

        mock_variable_get.return_value = token

        operator = YeeduJobRunOperator(
            task_id="test_task",
            job_conf_id=job_conf_id,
            hostname=hostname,
            workspace_id=workspace_id,
            token=None,
        )

        # Mocking requests.request
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"job_id": 789, "job_status": "ERROR"}
        mock_response.text = "syntax_error"
        mock_requests.return_value = mock_response

        with self.assertRaises(AirflowException) as context:
            operator.execute({})

        self.assertEqual(str(context.exception), "syntax_error")


if __name__ == "__main__":
    unittest.main()
