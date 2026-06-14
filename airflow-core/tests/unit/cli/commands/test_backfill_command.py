#
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

import argparse
from datetime import datetime
from types import SimpleNamespace

import pytest

import airflow.cli.commands.backfill_command
from airflow._shared.timezones import timezone
from airflow.cli import cli_parser
from airflow.models.backfill import ReprocessBehavior

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)


class TestCliBackfill:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def _base_args(self) -> list[str]:
        return [
            "backfill",
            "create",
            "--dag-id",
            "example_bash_operator",
            "--from-date",
            DEFAULT_DATE.isoformat(),
            "--to-date",
            DEFAULT_DATE.isoformat(),
        ]

    @pytest.mark.parametrize(
        ("repro", "expected_repro"),
        [
            (None, ReprocessBehavior.NONE),
            ("none", ReprocessBehavior.NONE),
            ("completed", ReprocessBehavior.COMPLETED),
            ("failed", ReprocessBehavior.FAILED),
        ],
    )
    def test_backfill(self, mock_cli_api_client, repro, expected_repro):
        args = self._base_args()
        if repro is not None:
            args.extend(["--reprocess-behavior", repro])
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_cli_api_client.backfills.create.assert_called_once()
        mock_cli_api_client.backfills.create_dry_run.assert_not_called()
        body = mock_cli_api_client.backfills.create.call_args.kwargs["backfill"]
        assert body.dag_id == "example_bash_operator"
        assert body.from_date == DEFAULT_DATE
        assert body.to_date == DEFAULT_DATE
        assert body.run_backwards is False
        assert body.dag_run_conf is None
        assert body.reprocess_behavior == expected_repro
        # Not passed on the command line; the API server resolves the fallback.
        assert body.run_on_latest_version is None

    def test_backfill_with_run_on_latest_version(self, mock_cli_api_client):
        args = [*self._base_args(), "--run-on-latest-version"]
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        body = mock_cli_api_client.backfills.create.call_args.kwargs["backfill"]
        assert body.run_on_latest_version is True

    @pytest.mark.parametrize("reverse", [False, True])
    def test_backfill_dry_run(self, mock_cli_api_client, reverse):
        mock_cli_api_client.backfills.create_dry_run.return_value = SimpleNamespace(
            backfills=[SimpleNamespace(logical_date=DEFAULT_DATE, partition_key=None, partition_date=None)]
        )
        args = [*self._base_args(), "--dry-run", "--reprocess-behavior", "none"]
        if reverse:
            args.append("--run-backwards")
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        mock_cli_api_client.backfills.create_dry_run.assert_called_once()
        mock_cli_api_client.backfills.create.assert_not_called()
        body = mock_cli_api_client.backfills.create_dry_run.call_args.kwargs["backfill"]
        assert body.run_backwards is reverse
        assert body.reprocess_behavior == ReprocessBehavior.NONE

    def test_backfill_with_dag_run_conf(self, mock_cli_api_client):
        args = [*self._base_args(), "--dag-run-conf", '{"example_key": "example_value"}']
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        body = mock_cli_api_client.backfills.create.call_args.kwargs["backfill"]
        assert body.dag_run_conf == {"example_key": "example_value"}

    def test_backfill_with_invalid_dag_run_conf(self, mock_cli_api_client):
        args = [*self._base_args(), "--dag-run-conf", '{"invalid": json}']
        with pytest.raises(ValueError, match="Invalid JSON in --dag-run-conf"):
            airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))
        mock_cli_api_client.backfills.create.assert_not_called()

    def test_backfill_with_empty_dag_run_conf(self, mock_cli_api_client):
        args = [*self._base_args(), "--dag-run-conf", "{}"]
        airflow.cli.commands.backfill_command.create_backfill(self.parser.parse_args(args))

        body = mock_cli_api_client.backfills.create.call_args.kwargs["backfill"]
        assert body.dag_run_conf == {}
