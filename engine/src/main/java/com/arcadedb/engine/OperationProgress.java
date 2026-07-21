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
package com.arcadedb.engine;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ProgressCallback;

/**
 * Live progress of one long-running operation, published in {@link OperationProgressRegistry} while the
 * operation runs. The producer thread updates the volatile fields through {@link #onProgress}; reader threads
 * (HTTP progress endpoint, console poller) take a weakly-consistent snapshot via the getters or
 * {@link #toJSON()}. Lock-free by design: updates are plain volatile writes of primitives, so the producer's
 * hot loop never blocks on a reader.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OperationProgress implements ProgressCallback {
  private final    long   id;
  private final    String databaseName;
  private final    String operation;
  private final    long   startedOn;
  private volatile String stepName   = "";
  private volatile int    stepIndex  = 0;
  private volatile int    totalSteps = 0;
  private volatile long   done       = 0L;
  private volatile long   total      = -1L;

  OperationProgress(final long id, final String databaseName, final String operation) {
    this.id = id;
    this.databaseName = databaseName;
    this.operation = operation;
    this.startedOn = System.currentTimeMillis();
  }

  @Override
  public void onProgress(final String stepName, final int stepIndex, final int totalSteps, final long done, final long total) {
    this.stepName = stepName;
    this.stepIndex = stepIndex;
    this.totalSteps = totalSteps;
    this.done = done;
    this.total = total;
  }

  public long getId() {
    return id;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getOperation() {
    return operation;
  }

  public String getStepName() {
    return stepName;
  }

  public int getStepIndex() {
    return stepIndex;
  }

  public int getTotalSteps() {
    return totalSteps;
  }

  public long getDone() {
    return done;
  }

  public long getTotal() {
    return total;
  }

  /** Percentage of the current step, or -1 when the step total is unknown. */
  public int getPercentage() {
    final long currentTotal = total;
    if (currentTotal <= 0)
      return -1;
    final long currentDone = done;
    return (int) Math.min(100L, currentDone * 100L / currentTotal);
  }

  public long getStartedOn() {
    return startedOn;
  }

  public JSONObject toJSON() {
    // READ EACH VOLATILE ONCE so done/total/percentage in the emitted JSON are mutually consistent. The
    // snapshot as a whole is still weakly consistent across fields (no lock is taken on the producer), which
    // is the documented contract and harmless for a progress display.
    final String currentStepName = stepName;
    final int currentStepIndex = stepIndex;
    final int currentTotalSteps = totalSteps;
    final long currentDone = done;
    final long currentTotal = total;

    final JSONObject json = new JSONObject();
    json.put("id", id);
    json.put("database", databaseName);
    json.put("operation", operation);
    json.put("stepName", currentStepName);
    json.put("stepIndex", currentStepIndex);
    json.put("totalSteps", currentTotalSteps);
    json.put("done", currentDone);
    json.put("total", currentTotal);
    json.put("percentage", currentTotal <= 0 ? -1 : (int) Math.min(100L, currentDone * 100L / currentTotal));
    json.put("startedOn", startedOn);
    json.put("elapsedMs", System.currentTimeMillis() - startedOn);
    return json;
  }
}
