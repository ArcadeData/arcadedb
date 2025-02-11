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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.utility.DateUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodFormat extends AbstractSQLMethod {

  public static final String NAME = "format";

  public SQLMethodFormat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(final Object value, final Identifiable iRecord, final CommandContext context, final Object[] params) {

    // TRY TO RESOLVE AS DYNAMIC VALUE
    String format = (String) getParameterValue(iRecord, params[0].toString());
    if (format == null)
      // USE STATIC ONE
      format = params[0].toString();

    if (format == null)
      throw new IllegalArgumentException("Format was null");

    if (isCollectionOfDates(value)) {
      final List<String> result = new ArrayList<String>();
      final Iterator<?> iterator = MultiValue.getMultiValueIterator(value);

      while (iterator.hasNext())
        result.add(DateUtils.format(iterator.next(), format));

      return result;

    } else if (DateUtils.isDate(value)) {
      return DateUtils.format(value, format, params.length > 1 ? (String) params[1] : null);
    }
    return value != null ? format.formatted(value) : null;
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
