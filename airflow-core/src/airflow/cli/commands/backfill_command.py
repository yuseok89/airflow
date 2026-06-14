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

import json

from tabulate import tabulate

from airflow.api_fastapi.core_api.datamodels.backfills import BackfillPostBody
from airflow.cli.api_client import NEW_API_CLIENT, Client, provide_api_client
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import deprecated_for_airflowctl
from airflow.models.backfill import ReprocessBehavior
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@cli_utils.action_cli
@deprecated_for_airflowctl("airflowctl backfill create")
@providers_configuration_loaded
@provide_api_client
def create_backfill(args, api_client: Client = NEW_API_CLIENT) -> None:
    """Create a backfill job, or dry run one, for a Dag through the API server."""
    console = AirflowConsole()

    dag_run_conf = None
    if args.dag_run_conf:
        try:
            dag_run_conf = json.loads(args.dag_run_conf)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in --dag-run-conf: {e}")

    # ``run_on_latest_version`` and the triggering user are resolved server-side; the
    # core_api request model is wire-compatible with the airflowctl client's generated model.
    body_kwargs: dict = dict(
        dag_id=args.dag_id,
        from_date=args.from_date,
        to_date=args.to_date,
        run_backwards=args.run_backwards,
        dag_run_conf=dag_run_conf,
        run_on_latest_version=args.run_on_latest_version,
    )
    if args.reprocess_behavior is not None:
        body_kwargs["reprocess_behavior"] = ReprocessBehavior(args.reprocess_behavior)
    if args.max_active_runs is not None:
        body_kwargs["max_active_runs"] = args.max_active_runs
    backfill_body = BackfillPostBody(**body_kwargs)

    if args.dry_run:
        console.print("Performing dry run of backfill.")
        dry_run = api_client.backfills.create_dry_run(backfill=backfill_body)  # type: ignore[arg-type]
        rows = [
            dict(
                logical_date=run.logical_date,
                partition_key=run.partition_key,
                partition_date=run.partition_date,
            )
            for run in dry_run.backfills
        ]
        console.print("Runs to be attempted:")
        console.print(tabulate(rows, tablefmt="grid", headers="keys"))
        return

    api_client.backfills.create(backfill=backfill_body)  # type: ignore[arg-type]
