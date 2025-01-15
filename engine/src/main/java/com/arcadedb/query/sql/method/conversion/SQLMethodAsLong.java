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

import java.time.temporal.*;
import java.util.*;

/**
 * Returns a number as a long (signed 32 bit representation).
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsLong extends AbstractSQLMethod {

  public static final String NAME = "aslong";

  public SQLMethodAsLong() {
    super(NAME);
  }

  @Override
  public Object execute(Object value, final Identifiable iCurrentRecord, final CommandContext iContext, final Object[] iParams) {
    if (value instanceof Number number)
      value = number.longValue();
    else if (value instanceof Date date)
      value = date.getTime();
    else if (DateUtils.isDate(value))
      value = DateUtils.dateTimeToTimestamp(value, ChronoUnit.MILLIS);
    else
      value = value != null ? Long.valueOf(value.toString().trim()) : null;

    return value;
  }
}
