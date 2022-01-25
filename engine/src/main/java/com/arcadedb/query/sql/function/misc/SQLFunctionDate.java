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
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Builds a date object from the format passed. If no arguments are passed, than the system date is built (like sysdate() function)
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see SQLFunctionSysdate
 */
public class SQLFunctionDate extends SQLFunctionAbstract {
  public static final String NAME = "date";

  private final Date             date;
  private       SimpleDateFormat format;

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionDate() {
    super(NAME);
    date = new Date();
  }

  public Object execute(Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    if (iParams.length == 0)
      return date;

    if (iParams[0] == null)
      return null;

    if (iParams[0] instanceof Number)
      return new Date(((Number) iParams[0]).longValue());

    if (format == null) {
      final TimeZone tz =
          iParams.length > 2 ? TimeZone.getTimeZone(iParams[2].toString()) : iContext.getDatabase().getSchema().getTimeZone();

      if (iParams.length > 1)
        format = new SimpleDateFormat((String) iParams[1]);
      else
        format = new SimpleDateFormat(iContext.getDatabase().getSchema().getDateFormat());

      format.setTimeZone(tz);
    }

    try {
      return format.parse((String) iParams[0]);
    } catch (ParseException e) {
      throw new QueryParsingException("Error on formatting date '" + iParams[0] + "' using the format: " + format.toPattern(), e);
    }
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "date([<date-as-string>] [,<format>] [,<timezone>])";
  }

  @Override
  public Object getResult() {
    format = null;
    return null;
  }
}
