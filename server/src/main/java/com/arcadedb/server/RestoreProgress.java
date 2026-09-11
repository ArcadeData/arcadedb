/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server;

import com.arcadedb.engine.OperationProgress;
import com.arcadedb.utility.ProgressCallback;

/**
 * What the server's two restore paths have to agree on to publish one operation to
 * {@code OperationProgressRegistry}: the step labels an operator reads, and the one way the archive extractor's
 * own counters are wired into a multi-step operation.
 * <p>
 * There are exactly two such paths and they are NOT variants of each other:
 * {@code ServerControlPlane.performRestore} serves the HTTP and gRPC {@code restore database} / {@code restore
 * backup} verbs and runs in three steps (extract, swap the temporary directory in, replicate to the cluster),
 * while {@link ArcadeDBServer#restoreDatabaseFromStartupCommand} serves the {@code restore:} command of
 * {@code arcadedb.server.defaultDatabases} and runs in two (extract, open) - it restores straight into the final
 * directory and forces no cluster snapshot. Only the step COUNT differs, which is why
 * {@link #installCallback} takes it as a parameter rather than each path keeping its own copy of the wiring
 * (issue #7440).
 * <p>
 * The labels are deliberately a copy of {@code AbstractRestoreFormat.RESTORE_STEP_NAME} rather than a reference
 * to it: {@code arcadedb-integration} is an optional dependency reached only reflectively, so the server cannot
 * name its constants at compile time. The two only have to agree so that the marker published before the
 * restorer exists reads the same as the reports the restorer then sends; a drift costs a changed label mid-step
 * and nothing more.
 */
final class RestoreProgress {
  /** Extracting the archive. Always step 1, and the only step the integration module itself reports on. */
  static final String STEP_EXTRACT  = "Restoring files";
  /** Making the extracted files the live database - a directory swap on the control plane, an open at startup. */
  static final String STEP_ACTIVATE = "Activating database";

  private RestoreProgress() {
  }

  /**
   * Installs {@code progress} as {@code restorer}'s progress callback, renumbering the format's own step - which
   * is always 1 of 1, because the integration module knows nothing of the phases the server runs after it - into
   * step 1 of the caller's {@code totalSteps}. Without this the registry would only ever carry the coarse step
   * markers the caller publishes itself; with it, the extractor's real per-entry counters reach it too.
   * <p>
   * Best-effort: a build of {@code arcadedb-integration} without the setter reports no counters rather than
   * failing the restore. Progress is a convenience; the restore is what the caller asked for.
   *
   * @param restoreClass {@code com.arcadedb.integration.restore.Restore}, resolved reflectively by the caller
   * @param restorer     the instance of it the caller is about to run
   * @param progress     the registry entry the caller registered for the whole operation
   * @param totalSteps   how many steps the CALLER publishes - 3 for the control plane, 2 for the startup command
   */
  static void installCallback(final Class<?> restoreClass, final Object restorer, final OperationProgress progress,
      final int totalSteps) {
    final ProgressCallback callback =
        (stepName, stepIndex, steps, done, total) -> progress.onProgress(stepName, 1, totalSteps, done, total);
    try {
      restoreClass.getMethod("setProgressCallback", ProgressCallback.class).invoke(restorer, callback);
    } catch (final ReflectiveOperationException ignored) {
      // No setter on this build: the coarse step markers the caller publishes are still there.
    }
  }
}
