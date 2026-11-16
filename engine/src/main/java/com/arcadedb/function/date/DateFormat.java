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

import java.util.Date;

/**
 * date.format(time, unit = 'ms', format = 'yyyy-MM-dd HH:mm:ss', timezone = 'UTC') - Format a timestamp to a string, as
 * APOC's apoc.date.format does (issue #12106): a {@link java.text.SimpleDateFormat} pattern, the timestamp read in
 * {@code unit} (saturating at the long range), rendered in {@code timezone}.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateFormat extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "format";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Format a timestamp to a string: date.format(time, unit = 'ms', format = 'yyyy-MM-dd HH:mm:ss', timezone = 'UTC')";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long millis = timeUnit(args.length > 1 ? args[1] : null).toMillis(toMillis(args[0]));
    return simpleDateFormat(patternArgument(args, 2), args.length > 3 ? args[3] : null).format(new Date(millis));
  }
}
