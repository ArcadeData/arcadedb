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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * Expression representing EXISTS predicate.
 * Examples:
 * - EXISTS { MATCH (n)-[:KNOWS]->(m) WHERE m.name = 'Alice' }
 * - EXISTS { (n)-[:KNOWS]->() }
 * <p>
 * Returns true if the pattern/subquery has at least one match, false otherwise.
 */
public class ExistsExpression implements Expression {
  private final String subquery;
  private final String text;

  public ExistsExpression(final String subquery, final String text) {
    this.subquery = subquery;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // For now, execute the subquery and check if it returns any results
    // Note: This is a simplified implementation. A more sophisticated version
    // would need to handle variable scoping from outer query.

    try {
      final var resultSet = context.getDatabase().query("opencypher", subquery);
      final boolean exists = resultSet.hasNext();
      resultSet.close();
      return exists;
    } catch (final Exception e) {
      // If the subquery fails to parse/execute, return false
      return false;
    }
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public String getSubquery() {
    return subquery;
  }
}
