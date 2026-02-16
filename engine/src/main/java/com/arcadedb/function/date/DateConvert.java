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
 * date.convert(timestamp, fromUnit, toUnit) - Convert timestamp between units.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateConvert extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "convert";
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
    return "Convert a timestamp from one unit to another";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long value = args[0] instanceof Number ? ((Number) args[0]).longValue() : Long.parseLong(args[0].toString());
    final String fromUnit = args[1] != null ? args[1].toString() : UNIT_MS;
    final String toUnit = args[2] != null ? args[2].toString() : UNIT_MS;

    // Convert to milliseconds first, then to target unit
    final long millis = value * unitToMillis(fromUnit);
    return millis / unitToMillis(toUnit);
  }
}
