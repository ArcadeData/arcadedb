/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.utility.DateUtils;

import java.util.*;

/**
 * Transforms a value to date. If the conversion is not possible, null is returned.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsDate extends AbstractSQLMethod {

  public static final String NAME = "asdate";

  public SQLMethodAsDate() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "asDate([<format>])";
  }

  @Override
  public Object execute(final Object value, final Identifiable iCurrentRecord, final CommandContext context, final Object[] iParams) {
    if (value == null)
      return null;

    if (value instanceof Date)
      return value;
    else if (value instanceof Number number)
      return new Date(number.longValue());

    final String format = iParams.length > 0 ? iParams[0].toString() : context.getDatabase().getSchema().getDateFormat();
    return DateUtils.getDate(DateUtils.parse(value.toString(), format), context.getDatabase().getSerializer().getDateImplementation());
  }
}
