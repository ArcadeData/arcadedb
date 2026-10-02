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
package com.arcadedb.function.sql.time;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.utility.DateUtils;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Returns the current date time. If the `zoneid` parameter is passed, then a ZonedDateTime instance is returned, in that zone, otherwise a
 * LocalDateTime holding the UTC wall clock, which is how the engine stores a DATETIME (issue #8892).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see SQLFunctionDate
 */
public class SQLFunctionSysdate extends SQLFunctionAbstract {
  public static final String NAME = "sysdate";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionSysdate() {
    super(NAME);
  }

  public Object execute(final Object thisObject, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    // The clock is read as an INSTANT and handed over in the engine's convention: a LocalDateTime is stored as a UTC
    // wall clock, so the JVM-local wall clock (LocalDateTime.now()) landed hours away from the real instant on any
    // non-UTC JVM (issue #8892).
    Object result = LocalDateTime.now(ZoneOffset.UTC);

    // The zone is the FIRST argument - `sysdate([<zoneid>])`. Reading params[1] meant the one-argument form the
    // syntax documents silently dropped the zone and answered server-local time (issue #6388).
    if (params.length > 0 && params[0] != null) {
      final String zoneId = params[0].toString();
      try {
        result = ZonedDateTime.now(ZoneId.of(zoneId));
      } catch (final DateTimeException e) {
        throw new IllegalArgumentException(NAME + "() received an unknown time zone id '" + zoneId + "'", e);
      }
    }

    return DateUtils.getDate(result, context.getDatabase().getSerializer().getDateTimeImplementation());
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  public String getSyntax() {
    return "sysdate([<zoneid>])";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
