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
package com.arcadedb.query.opencypher.functions.util;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * util.sleep(ms) - Sleep for the specified number of milliseconds.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class UtilSleep extends AbstractUtilFunction {
  @Override
  protected String getSimpleName() {
    return "sleep";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Sleep for the specified number of milliseconds";
  }

  private static final long MAX_SLEEP_MS = 60000; // 1 minute maximum

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long milliseconds;
    if (args[0] instanceof Number) {
      milliseconds = ((Number) args[0]).longValue();
    } else {
      milliseconds = Long.parseLong(args[0].toString());
    }

    if (milliseconds <= 0)
      return null;

    if (milliseconds > MAX_SLEEP_MS) {
      throw new IllegalArgumentException(
          "Sleep duration exceeds maximum allowed (" + MAX_SLEEP_MS + "ms): " + milliseconds + "ms");
    }

    try {
      Thread.sleep(milliseconds);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Sleep interrupted", e);
    }

    return null;
  }
}
