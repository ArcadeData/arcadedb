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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Returns the current date time.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see SQLFunctionDate
 */
public class SQLFunctionSysdate extends SQLFunctionAbstract {
  public static final String NAME = "sysdate";

  private final Date             now;
  private       SimpleDateFormat format;

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionSysdate() {
    super(NAME);
    now = new Date();
  }

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    if (iParams.length == 0)
      return now;

    if (format == null) {
      final TimeZone tz =
          iParams.length > 0 ? TimeZone.getTimeZone(iParams[0].toString()) : iContext.getDatabase().getSchema().getTimeZone();

      if (iParams.length > 1)
        format = new SimpleDateFormat((String) iParams[1]);
      else
        format = new SimpleDateFormat(iContext.getDatabase().getSchema().getDateFormat());

      format.setTimeZone(tz);
    }

    return format.format(now);
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "sysdate([<format>] [,<timezone>])";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
