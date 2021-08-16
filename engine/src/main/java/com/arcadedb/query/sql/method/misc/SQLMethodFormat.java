/*
 * Copyright 2021 Arcade Data Ltd
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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodFormat extends OAbstractSQLMethod {

  public static final String NAME = "format";

  public SQLMethodFormat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iRecord, final CommandContext iContext, Object ioResult,
      final Object[] iParams) {

    // TRY TO RESOLVE AS DYNAMIC VALUE
    Object v = getParameterValue(iRecord, iParams[0].toString());
    if (v == null)
      // USE STATIC ONE
      v = iParams[0].toString();

    if (v != null) {
      if (isCollectionOfDates(ioResult)) {
        List<String> result = new ArrayList<String>();
        Iterator<Object> iterator = MultiValue.getMultiValueIterator(ioResult);
        final SimpleDateFormat format = new SimpleDateFormat(v.toString());

        final TimeZone tz =
            iParams.length > 1 ? TimeZone.getTimeZone(iParams[1].toString()) : iContext.getDatabase().getSchema().getTimeZone();

        format.setTimeZone(tz);

        while (iterator.hasNext()) {
          result.add(format.format(iterator.next()));
        }
        ioResult = result;
      } else if (ioResult instanceof Date) {
        final SimpleDateFormat format = new SimpleDateFormat(v.toString());
        final TimeZone tz =
            iParams.length > 1 ? TimeZone.getTimeZone(iParams[1].toString()) : iContext.getDatabase().getSchema().getTimeZone();

        format.setTimeZone(tz);
        ioResult = format.format(ioResult);
      } else {
        ioResult = ioResult != null ? String.format(v.toString(), ioResult) : null;
      }
    }
    return ioResult;
  }

  private boolean isCollectionOfDates(Object ioResult) {
    if (MultiValue.isMultiValue(ioResult)) {
      Iterator<Object> iterator = MultiValue.getMultiValueIterator(ioResult);
      while (iterator.hasNext()) {
        Object item = iterator.next();
        if (item != null && !(item instanceof Date)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
