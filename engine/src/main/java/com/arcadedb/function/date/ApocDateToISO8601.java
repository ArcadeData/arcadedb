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
 * apoc.date.toISO8601(time, unit = 'ms') - A timestamp as an ISO 8601 string in UTC with millisecond precision
 * ({@code 2020-09-13T12:26:40.000Z}), as APOC's apoc.date.toISO8601 renders it (issue #12106). ArcadeDB's own date.toISO8601,
 * in the JVM's zone with its offset, is {@link DateToISO8601}.
 */
public class ApocDateToISO8601 extends AbstractApocDateFunction {
  private static final String ISO_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

  @Override
  protected String getSimpleName() {
    return "toISO8601";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Convert a timestamp to an ISO 8601 string in UTC with milliseconds as APOC does: apoc.date.toISO8601(time, unit = 'ms')";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long millis = timeInMillis(args[0], args.length > 1 ? args[1] : null);
    return simpleDateFormat(ISO_PATTERN, UTC).format(new Date(millis));
  }
}
