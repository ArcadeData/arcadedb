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
package com.arcadedb.utility;

/**
 * Receives progress notifications from long-running maintenance operations (CHECK DATABASE, index rebuilds,
 * etc.). Implementations must be cheap and non-blocking: producers invoke the callback from their hot loop
 * (already throttled to integer-percentage changes) and pass only primitives plus interned/step-constant
 * strings, so a well-behaved implementation causes no garbage-collector pressure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@FunctionalInterface
public interface ProgressCallback {
  /**
   * @param stepName   human-readable name of the current step (e.g. "Checking vertices 'Account'")
   * @param stepIndex  1-based index of the current step
   * @param totalSteps total number of steps of the whole operation
   * @param done       units of work completed in the current step
   * @param total      total units of work of the current step, or -1 when unknown
   */
  void onProgress(String stepName, int stepIndex, int totalSteps, long done, long total);
}
