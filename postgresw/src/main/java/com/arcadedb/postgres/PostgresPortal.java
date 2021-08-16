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
package com.arcadedb.postgres;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.parser.Statement;

import java.util.List;
import java.util.Map;

public class PostgresPortal {
  public String                    query;
  public List<Long>                parameterTypes;
  public List<Integer>             parameterFormats;
  public List<Object>              parameterValues;
  public List<Integer>             resultFormats;
  public Statement                 statement;
  public boolean                   ignoreExecution   = false;
  public List<Result>              cachedResultset;
  public Map<String, PostgresType> columns;
  public boolean                   isExpectingResult = true;
  public boolean                   executed          = false;

  public PostgresPortal(final String query) {
    this.query = query;
    //final String queryUpperCase = query.toUpperCase();
    this.isExpectingResult = true;//queryUpperCase.startsWith("SELECT") || queryUpperCase.startsWith("MATCH");
  }

  @Override
  public String toString() {
    return query;
  }
}
