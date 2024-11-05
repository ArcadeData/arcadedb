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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Transform the element in a collections or map.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodTransform extends AbstractSQLMethod {

  public static final  String   NAME       = "transform";
  private static final Object[] EMPTY_ARGS = new Object[] {};

  public SQLMethodTransform() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "transform()";
  }

  @Override
  public Object execute(final Object value, final Identifiable iCurrentRecord, final CommandContext iContext,
      final Object[] iParams) {
    if (value == null || iParams == null || iParams.length == 0)
      return null;

    final DefaultSQLMethodFactory methodFactory = ((SQLQueryEngine) iContext.getDatabase()
        .getQueryEngine("SQL")).getMethodFactory();
    final List<SQLMethod> transformers = new ArrayList<>(iParams.length);
    for (Object o : iParams) {
      if (o == null)
        throw new CommandSQLParsingException("Null argument in arguments for transform() method");
      transformers.add(methodFactory.createMethod(o.toString()));
    }

    if (value instanceof List list) {
      final List<Object> newList = new ArrayList<>(list.size());
      for (Object o : list) {
        Object transformed = o;

        for (SQLMethod m : transformers)
          transformed = m.execute(transformed, null, iContext, EMPTY_ARGS);

        newList.add(transformed);
      }
      return newList;
    } else if (value instanceof Set set) {
      final Set<Object> newSet = new HashSet<>(set.size());
      for (Object o : set) {
        Object transformed = o;

        for (SQLMethod m : transformers)
          transformed = m.execute(transformed, null, iContext, EMPTY_ARGS);

        newSet.add(transformed);
      }
      return newSet;
    }

    return value;
  }
}
