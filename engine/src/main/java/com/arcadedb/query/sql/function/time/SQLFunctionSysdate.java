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

import java.time.*;

/**
 * Returns the current date time. If the `zoneid` parameter is passed, then a ZonedDateTime instance is returned, otherwise a LocalDateTime.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
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

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    final LocalDateTime now = LocalDateTime.now();
    Object result = now;

    if (iParams.length > 0) {
      if (iParams.length > 1)
        result = now.atZone(ZoneId.of(iParams[1].toString()));
    }

    return DateUtils.getDate(result, iContext.getDatabase().getSerializer().getDateTimeImplementation());
  }

  public String getSyntax() {
    return "sysdate([<zoneid>])";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
