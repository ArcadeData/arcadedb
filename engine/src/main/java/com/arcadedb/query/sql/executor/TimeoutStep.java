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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Timeout;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class TimeoutStep extends AbstractExecutionStep {
  private final Timeout timeout;

  private Long expiryTime;

  public TimeoutStep(Timeout timeout, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (this.expiryTime == null) {
      this.expiryTime = System.currentTimeMillis() + timeout.getVal().longValue();
    }
    if (System.currentTimeMillis() > expiryTime) {
      return fail();
    }
    return getPrev().get().syncPull(ctx, nRecords);//TODO do it more granular
  }

  private ResultSet fail() {
    this.timedOut = true;
    sendTimeout();
    if (Timeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      return new InternalResultSet();
    } else {
      throw new TimeoutException("Timeout expired");
    }
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new TimeoutStep(this.timeout.copy(), ctx, profilingEnabled);
  }
}
