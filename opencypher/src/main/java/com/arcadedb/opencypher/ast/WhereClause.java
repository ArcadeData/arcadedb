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
package com.arcadedb.opencypher.ast;

/**
 * Represents a WHERE clause in a Cypher query.
 * Contains filter expressions to apply to matched patterns.
 */
public class WhereClause {
  private final BooleanExpression condition;

  // Legacy constructor for backward compatibility (deprecated)
  private final String conditionString;

  public WhereClause(final BooleanExpression condition) {
    this.condition = condition;
    this.conditionString = condition != null ? condition.getText() : null;
  }

  // Legacy constructor - kept for backward compatibility
  public WhereClause(final String condition) {
    this.conditionString = condition;
    this.condition = null;
  }

  public BooleanExpression getConditionExpression() {
    return condition;
  }

  // Legacy method - kept for backward compatibility
  public String getCondition() {
    return conditionString;
  }
}
