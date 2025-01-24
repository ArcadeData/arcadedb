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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Extracts a sub string from the original.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodSubString extends AbstractSQLMethod {

  public static final String NAME = "substring";

  public SQLMethodSubString() {
    super(NAME, 1, 2);
  }

  @Override
  public String getSyntax() {
    return "subString(<from-index> [,<to-index>])";
  }

  @Override
  public Object execute( final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    if (value == null || params[0] == null) {
      return null;
    }

    if (params.length > 1) {
      int from = Integer.parseInt(params[0].toString());
      int to = Integer.parseInt(params[1].toString());
      final String thisString = value.toString();
      if (from < 0) {
        from = 0;
      }
      if (from >= thisString.length()) {
        return "";
      }
      if (to > thisString.length()) {
        to = thisString.length();
      }
      if (to <= from) {
        return "";
      }

      return thisString.substring(from, to);
    } else {
      int from = Integer.parseInt(params[0].toString());
      final String thisString = value.toString();
      if (from < 0) {
        from = 0;
      }
      if (from >= thisString.length()) {
        return "";
      }
      return thisString.substring(from);
    }
  }
}
