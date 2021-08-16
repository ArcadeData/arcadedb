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

package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Record;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;

public interface OIndexableSQLFunction {
  boolean shouldExecuteAfterSearch(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context,
      Expression[] oExpressions);

  boolean allowsIndexedExecution(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context,
      Expression[] oExpressions);

  boolean canExecuteInline(FromClause target, BinaryCompareOperator operator, Object right, CommandContext context,
      Expression[] oExpressions);

  long estimate(FromClause target, BinaryCompareOperator operator, Object rightValue, CommandContext ctx,
      Expression[] oExpressions);

  Iterable<Record> searchFromTarget(FromClause target, BinaryCompareOperator operator, Object rightValue, CommandContext ctx,
      Expression[] oExpressions);
}
