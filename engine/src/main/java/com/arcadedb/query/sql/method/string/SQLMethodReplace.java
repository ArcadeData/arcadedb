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
 * Replaces all the occurrences.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodReplace extends AbstractSQLMethod {

  public static final String NAME = "replace";

  public SQLMethodReplace() {
    super(NAME, 2, 2);
  }

  @Override
  public String getSyntax() {
    return "replace(<to-find>, <to-replace>)";
  }

  @Override
  public Object execute( final Object value, final Identifiable currentRecord,
      final CommandContext context, final Object[] params) {
    if (value == null || params[0] == null || params[1] == null)
      return params[0];

    return value.toString().replace(params[0].toString(), params[1].toString());
  }
}
