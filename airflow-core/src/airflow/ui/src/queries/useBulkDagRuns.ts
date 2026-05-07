/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useDagRunServiceGetDagRunsKey, useTaskInstanceServiceGetTaskInstancesKey } from "openapi/queries";
import { DagRunService } from "openapi/requests/services.gen";
import type { DAGRunPatchStates, DAGRunResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

// NOTE: Until a real bulk Dag Runs API endpoint exists (analogous to
// `bulkTaskInstances` / `bulkVariables`), the actions below fan out to one
// request per Dag Run via Promise.allSettled. This means up to N (or 2N for
// clear-with-note) concurrent writes when the user selects N runs and partial
// failures are surfaced after the fact.

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

export type BulkClearDagRunsOptions = {
  note: string | null;
  onlyFailed: boolean;
  onlyNew: boolean;
  runOnLatestVersion: boolean;
};

type BulkMarkOptions = {
  note: string | null;
  state: DAGRunPatchStates;
};

type ToasterKey = "toaster.bulkClear" | "toaster.bulkDelete" | "toaster.bulkUpdate";

export const useBulkDagRuns = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const [isPending, setIsPending] = useState(false);
  const { t: translate } = useTranslation(["common", "dags"]);

  const invalidateQueries = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
      queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
    ]);
  };

  const finalize = (
    dagRuns: Array<DAGRunResponse>,
    results: Array<PromiseSettledResult<unknown>>,
    toasterKey: ToasterKey,
  ) => {
    const successDagRuns = dagRuns.filter((_, index) => results[index]?.status === "fulfilled");
    const firstRejection = results.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );

    if (firstRejection) {
      setError(firstRejection.reason);
    } else {
      setError(undefined);
    }

    if (successDagRuns.length > 0) {
      toaster.create({
        description: translate(`${toasterKey}.success.description`, {
          count: successDagRuns.length,
          keys: successDagRuns.map((dr) => dr.dag_run_id).join(", "),
          resourceName: translate("dagRun_other"),
        }),
        title: translate(`${toasterKey}.success.title`, {
          resourceName: translate("dagRun_other"),
        }),
        type: "success",
      });
    }

    if (!firstRejection) {
      clearSelections();
      onSuccessConfirm();
    }
  };

  const bulkClear = async (dagRuns: Array<DAGRunResponse>, options: BulkClearDagRunsOptions) => {
    setError(undefined);
    setIsPending(true);

    try {
      const clearResults = await Promise.allSettled(
        dagRuns.map((dagRun) =>
          DagRunService.clearDagRun({
            dagId: dagRun.dag_id,
            dagRunId: dagRun.dag_run_id,
            requestBody: {
              dry_run: false,
              only_failed: options.onlyFailed,
              only_new: options.onlyNew,
              run_on_latest_version: options.runOnLatestVersion,
            },
          }),
        ),
      );

      // Only patch notes for runs whose clear succeeded. Patch failures here
      // are intentionally swallowed so a flaky note write does not undo the
      // visible "successful clear" outcome; we still report the original
      // clear-side rejections via finalize().
      if (options.note !== null) {
        const toPatch = dagRuns.filter((_, index) => clearResults[index]?.status === "fulfilled");

        if (toPatch.length > 0) {
          await Promise.allSettled(
            toPatch.map((dagRun) =>
              DagRunService.patchDagRun({
                dagId: dagRun.dag_id,
                dagRunId: dagRun.dag_run_id,
                requestBody: { note: options.note },
                updateMask: ["note"],
              }),
            ),
          );
        }
      }

      await invalidateQueries();
      finalize(dagRuns, clearResults, "toaster.bulkClear");
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  const bulkDelete = async (dagRuns: Array<DAGRunResponse>) => {
    setError(undefined);
    setIsPending(true);

    try {
      const results = await Promise.allSettled(
        dagRuns.map((dagRun) =>
          DagRunService.deleteDagRun({
            dagId: dagRun.dag_id,
            dagRunId: dagRun.dag_run_id,
          }),
        ),
      );

      await invalidateQueries();
      finalize(dagRuns, results, "toaster.bulkDelete");
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  const bulkMarkAs = async (dagRuns: Array<DAGRunResponse>, options: BulkMarkOptions) => {
    setError(undefined);
    setIsPending(true);

    const updateMask = options.note === null ? ["state"] : ["state", "note"];

    try {
      const results = await Promise.allSettled(
        dagRuns.map((dagRun) =>
          DagRunService.patchDagRun({
            dagId: dagRun.dag_id,
            dagRunId: dagRun.dag_run_id,
            requestBody: { note: options.note, state: options.state },
            updateMask,
          }),
        ),
      );

      await invalidateQueries();
      finalize(dagRuns, results, "toaster.bulkUpdate");
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  return { bulkClear, bulkDelete, bulkMarkAs, error, isPending, setError };
};
