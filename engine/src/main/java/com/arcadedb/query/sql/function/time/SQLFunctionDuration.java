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
package com.arcadedb.query.sql.function.time;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.utility.DateUtils;

import java.time.Duration;

/**
 * Returns a java.time.Duration.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see {@link SQLFunctionSysdate}, {@link SQLFunctionDate}
 */
public class SQLFunctionDuration extends SQLFunctionAbstract {
  public static final String NAME = "duration";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionDuration() {
    super(NAME);
  }

  public Object execute(final Object thisObject, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    if (params.length != 2)
      throw new IllegalArgumentException("duration() function expected 2 parameters: amount and time-unit");

    long amount = getAmount(params[0]);

    return Duration.of(amount, DateUtils.parsePrecision(params[1].toString()));
  }

  private static long getAmount(Object param) {
    if (param instanceof Number number)
      return number.longValue();
    else if (param instanceof String string)
      return Long.parseLong(string);
    else
      throw new IllegalArgumentException("amount '" + param + "' not a number or a string");
  }

  public String getSyntax() {
    return "duration(<amount>, <time-unit>)";
  }
}
