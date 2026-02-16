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
package com.arcadedb.function.date;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * date.add(timestamp, value, unit) - Add time to timestamp.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateAdd extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "add";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Add a specified amount of time to a timestamp";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long timestamp = toMillis(args[0]);
    final long value = args[1] instanceof Number ? ((Number) args[1]).longValue() : Long.parseLong(args[1].toString());
    final String unit = args[2] != null ? args[2].toString() : UNIT_MS;

    try {
      // Use Math.multiplyExact and Math.addExact to prevent integer overflow
      final long addMillis = Math.multiplyExact(value, unitToMillis(unit));
      return Math.addExact(timestamp, addMillis);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException("Date arithmetic overflow: " + e.getMessage(), e);
    }
  }
}
