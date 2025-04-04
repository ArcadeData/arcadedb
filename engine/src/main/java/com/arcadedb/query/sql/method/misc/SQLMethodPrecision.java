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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.utility.DateUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;

import static java.time.temporal.ChronoUnit.MILLIS;

/**
 * Modifies the precision of a datetime.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodPrecision extends AbstractSQLMethod {

  public static final String NAME = "precision";

  public SQLMethodPrecision() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (params == null || params.length == 0 || params[0] == null)
      throw new IllegalArgumentException("precision method was expecting the time unit");

    final ChronoUnit targetPrecision = DateUtils.parsePrecision(params[0].toString());

    if (value instanceof LocalDateTime localDateTime)
      return localDateTime.truncatedTo(targetPrecision);
    else if (value instanceof ZonedDateTime zonedDateTime)
      return zonedDateTime.truncatedTo(targetPrecision);
    else if (value instanceof Instant instant)
      return instant.truncatedTo(targetPrecision);
    else if (value instanceof Date date) {
      if (targetPrecision == MILLIS)
        return value;
      return DateUtils.dateTime(context.getDatabase(), date.getTime(), MILLIS, LocalDateTime.class, targetPrecision);
    } else if (value instanceof Calendar calendar) {
      if (targetPrecision == MILLIS)
        return calendar;
      return DateUtils.dateTime(context.getDatabase(), calendar.getTimeInMillis(), MILLIS, LocalDateTime.class, targetPrecision);
    }

    throw new CommandExecutionException("Error on changing precision for unsupported type '" + value.getClass() + "'");
  }
}
