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

import java.text.DecimalFormat;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public abstract class AbstractExecutionStep implements ExecutionStepInternal {

  protected final CommandContext                  ctx;
  protected       Optional<ExecutionStepInternal> prev     = Optional.empty();
  protected       Optional<ExecutionStepInternal> next     = Optional.empty();
  protected       boolean                         timedOut = false;

  protected boolean profilingEnabled;

  public AbstractExecutionStep(CommandContext ctx, boolean profilingEnabled) {
    this.ctx = ctx;
    this.profilingEnabled = profilingEnabled;
  }

  @Override
  public void setPrevious(ExecutionStepInternal step) {
    this.prev = Optional.ofNullable(step);
  }

  @Override
  public void setNext(ExecutionStepInternal step) {
    this.next = Optional.ofNullable(step);
  }

  public CommandContext getContext() {
    return ctx;
  }

  public Optional<ExecutionStepInternal> getPrev() {
    return prev;
  }

  public Optional<ExecutionStepInternal> getNext() {
    return next;
  }

  @Override
  public void sendTimeout() {
    this.timedOut = true;
    prev.ifPresent(ExecutionStepInternal::sendTimeout);
  }

  public boolean isTimedOut() {
    return timedOut;
  }

  @Override
  public void close() {
    prev.ifPresent(ExecutionStepInternal::close);
  }

  public boolean isProfilingEnabled() {
    return profilingEnabled;
  }

  public void setProfilingEnabled(boolean profilingEnabled) {
    this.profilingEnabled = profilingEnabled;
  }

  protected String getCostFormatted() {
    return new DecimalFormat().format(getCost() / 1000) + "μs";
  }

}
