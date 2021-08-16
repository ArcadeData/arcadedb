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
package com.arcadedb.query.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Keeps items only once removing duplicates
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionDistinct extends SQLFunctionAbstract {
  public static final String NAME = "distinct";

  private Set<Object> context = new LinkedHashSet<Object>();

  public SQLFunctionDistinct() {
    super(NAME);
  }

  public Object execute(Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams, CommandContext iContext) {
    final Object value = iParams[0];

    if (value != null && !context.contains(value)) {
      context.add(value);
      return value;
    }

    return null;
  }

  public String getSyntax() {
    return "distinct(<field>)";
  }
}
