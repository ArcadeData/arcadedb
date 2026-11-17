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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.StringUtils;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodFormat extends AbstractSQLMethod {

  public static final String NAME = "format";

  public SQLMethodFormat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(final Object value, final Identifiable iRecord, final CommandContext context, final Object[] params) {

    if (params[0] == null)
      return null;

    // TRY TO RESOLVE AS DYNAMIC VALUE. The resolved field may hold anything, so it is rendered rather than cast:
    // a non-STRING field used to answer a ClassCastException here (issue #6389).
    final Object resolved = getParameterValue(iRecord, params[0].toString());
    // USE STATIC ONE WHEN THE FIELD DOES NOT RESOLVE
    final String format = resolved != null ? resolved.toString() : params[0].toString();

    final String timeZone = timeZoneOf(params);

    if (isCollectionOfDates(value)) {
      final List<String> result = new ArrayList<String>();
      final Iterator<?> iterator = MultiValue.getMultiValueIterator(value);

      // every element is rendered in the requested zone, as a single value is (issue #10676)
      while (iterator.hasNext())
        result.add(formatDate(iterator.next(), format, timeZone));

      return result;

    } else if (DateUtils.isDate(value)) {
      return formatDate(value, format, timeZone);
    }
    return value != null ? StringUtils.format(NAME, format, value) : null;
  }

  /**
   * The time zone argument, which must be text naming a zone. It used to be cast, so any other kind answered a
   * ClassCastException: a server fault for the caller's argument (issue #12020).
   */
  private static String timeZoneOf(final Object[] params) {
    if (params.length < 2 || params[1] == null)
      return null;
    if (params[1] instanceof String zone)
      return zone;
    throw new TypeMismatchException(
        NAME + "() expects a time zone name as its second argument, but received " + params[1].getClass().getSimpleName() + " "
            + InvalidArgumentException.quote(params[1]));
  }

  /**
   * A zone nothing names ({@code ZoneRulesException}), an offset out of range and a pattern field the value lacks
   * ({@code DateTimeException}), or a malformed pattern ({@code IllegalArgumentException}) are the caller's argument, not a
   * server fault (issue #12020).
   */
  private static String formatDate(final Object value, final String format, final String timeZone) {
    try {
      return DateUtils.format(value, format, timeZone);
    } catch (final DateTimeException | IllegalArgumentException e) {
      throw new InvalidArgumentException(
          NAME + "() cannot format the value with the pattern '" + InvalidArgumentException.quote(format) + "'" + (timeZone != null ?
              " in the time zone '" + InvalidArgumentException.quote(timeZone) + "'" :
              "") + ": " + e.getMessage(), e);
    }
  }

  private boolean isCollectionOfDates(final Object value) {
    if (MultiValue.isMultiValue(value)) {
      final Iterator<?> iterator = MultiValue.getMultiValueIterator(value);
      while (iterator.hasNext()) {
        final Object item = iterator.next();
        if (item != null && !DateUtils.isDate(item)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
