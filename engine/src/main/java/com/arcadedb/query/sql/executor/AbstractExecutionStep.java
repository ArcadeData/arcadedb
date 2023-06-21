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
package com.arcadedb.query.sql.executor;

import java.text.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public abstract class AbstractExecutionStep implements ExecutionStepInternal {
  public final static int                   DEFAULT_FETCH_RECORDS_PER_PULL = 100;
  protected final     CommandContext        context;
  protected           ExecutionStepInternal prev                           = null;
  protected           boolean               timedOut                       = false;
  protected           long                  cost                           = -1;
  protected final     boolean               profilingEnabled;

  public AbstractExecutionStep(final CommandContext context, final boolean profilingEnabled) {
    this.context = context;
    this.profilingEnabled = profilingEnabled;
  }

  @Override
  public void setPrevious(final ExecutionStepInternal step) {
    this.prev = step;
  }

  public CommandContext getContext() {
    return context;
  }

  public ExecutionStepInternal getPrev() {
    return prev;
  }

  @Override
  public void sendTimeout() {
    this.timedOut = true;
    if (prev != null)
      prev.sendTimeout();
  }

  public boolean isTimedOut() {
    return timedOut;
  }

  @Override
  public void close() {
    if (prev != null)
      prev.close();
  }

  @Override
  public long getCost() {
    return cost;
  }

  protected String getCostFormatted() {
    final long computedCost = getCost();
    return computedCost > -1 ? new DecimalFormat().format(computedCost / 1000) + "μs" : "";
  }

  protected ExecutionStepInternal checkForPrevious() {
    return checkForPrevious("filter step requires a previous step");
  }

  protected ExecutionStepInternal checkForPrevious(final String exceptionMessage) {
    if (prev == null)
      throw new IllegalStateException(exceptionMessage);
    return prev;
  }

  protected void pullPrevious(final CommandContext context, final int nRecords) {
    if (prev != null)
      prev.syncPull(context, nRecords);
  }
}
