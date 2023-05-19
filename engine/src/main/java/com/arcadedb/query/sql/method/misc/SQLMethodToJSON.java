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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

/**
 * Converts a document in JSON string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodToJSON extends AbstractSQLMethod {

  public static final String NAME = "tojson";

  public SQLMethodToJSON() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "toJSON()";
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult, final Object[] iParams) {
    if (iThis == null)
      return null;

    if (iThis instanceof Result) {
      return ((Result) iThis).toJSON();
    } else if (iThis instanceof Document) {
      return ((Document) iThis).toJSON();
    } else if (iThis instanceof Map) {
      return new JSONObject(iThis.toString());
    } else if (MultiValue.isMultiValue(iThis)) {
      final StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      for (final Object o : MultiValue.getMultiValueIterable(iThis, false)) {
        if (!first) {
          builder.append(",");
        }
        builder.append(execute(o, iCurrentRecord, iContext, ioResult, iParams));
        first = false;
      }

      builder.append("]");
      return builder.toString();
    }
    return null;
  }
}
