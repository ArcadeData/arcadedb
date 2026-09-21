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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.utility.DateUtils;

import java.time.format.DateTimeParseException;

/**
 * Transforms a value to datetime. If the conversion is not possible, null is returned.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodAsDateTime extends AbstractSQLMethod {

  public static final String NAME = "asdatetime";

  public SQLMethodAsDateTime() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "asDatetime([<format>])";
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;

    final Class dateTimeImpl = context.getDatabase().getSerializer().getDateTimeImplementation();

    if (DateUtils.isDate(value))
      return value;
    else if (value instanceof Number number)
      return DateUtils.getDate(value, dateTimeImpl);

    // With an explicit format the caller has said exactly how to read the string, so that pattern alone applies.
    // Without one, the shared chain applies - the same one the write path uses - so `asDatetime()` accepts every
    // spelling an INSERT accepts, including the SQL timestamp with a fractional second (issue #8090), instead of
    // only the schema's single dateTimeFormat pattern.
    //
    // A value that matches nothing answers null, as this method's contract has always promised and as the sibling
    // date() function already does. That is the READ side of the split issue #8090 drew: a write must refuse a
    // value it cannot store, because silently emptying the column is the data loss being fixed, while a conversion
    // asked for inside a query is an ordinary miss.
    final Object date;
    try {
      date = params.length > 0 ?
          DateUtils.parse(value.toString(), params[0].toString()) :
          DateUtils.parseDateTime(context.getDatabase(), value.toString());
    } catch (final DateTimeParseException e) {
      return null;
    }

    return DateUtils.getDate(date, dateTimeImpl);
  }
}
