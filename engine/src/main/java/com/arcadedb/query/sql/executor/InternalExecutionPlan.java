/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

/**
 * Created by luigidellaquila on 06/07/16.
 */
@ExcludeFromJacocoGeneratedReport
public interface InternalExecutionPlan extends ExecutionPlan {

  String JAVA_TYPE = "javaType";

  default void close() {
    // NO ACTION
  }

  /**
   * if the execution can still return N elements, then the result will contain them all. If the execution contains less than N
   * elements, then the result will contain them all, next result(s) will contain zero elements
   *
   * @param n
   *
   * @return
   */
  ResultSet fetchNext(int n);

  void reset(CommandContext context);

  default long getCost() {
    return -1;
  }

  default InternalExecutionPlan copy(final CommandContext context) {
    throw new UnsupportedOperationException();
  }

  boolean canBeCached();

  default String getStatement() {
    return null;
  }

  default void setStatements(final List<Statement> stm) {
    // NO ACTIONS
  }
}
