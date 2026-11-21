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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.UncorrelatedSubQuery;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.BaseIdentifier;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.SubQueryCollector;

import java.util.Map;

/**
 * Expression wrapper that executes a SELECT subquery when evaluated.
 * Used for left-side subqueries in IN conditions: (SELECT ...) IN collection, and for every parenthesized SELECT written as
 * an operand.
 * <p>
 * A sub-query that does not depend on the outer row (see {@link UncorrelatedSubQuery}) is lifted by the planner into a LET
 * that runs once ({@link #extractSubQueries}): from then on this node reads the LET variable, through the
 * {@link BaseExpression#identifier} it is given, instead of running the statement. It keeps {@link #statement} all the same,
 * because that is what it renders as - the default name of an unaliased projection column, and the EXPLAIN text, must not
 * change with the way the value is produced (issue #12714).
 */
public class SubqueryExpression extends BaseExpression {
  private final SelectStatement statement;

  public SubqueryExpression(final SelectStatement statement) {
    this.statement = statement;
  }

  public SelectStatement getStatement() {
    return statement;
  }

  /**
   * Lifts the sub-query into a LET of the enclosing statement, which the planner runs once, when it does not depend on the
   * outer row. Any other sub-query is left in place and runs for every row.
   */
  @Override
  public void extractSubQueries(final SubQueryCollector collector) {
    if (identifier == null && UncorrelatedSubQuery.isHoistable(statement))
      identifier = new BaseIdentifier(collector.addStatement(statement));
  }

  /** Never a bare column name, even once lifted: the variable it reads is an implementation detail, not the column. */
  @Override
  public boolean isBaseIdentifier() {
    return false;
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    if (identifier != null)
      return super.execute(currentRecord, context);

    Object result = executeSubquery(context);

    // Apply modifier if present (e.g., (SELECT ...).name or (SELECT ...)[0])
    if (modifier != null)
      result = modifier.execute(currentRecord, result, context);

    return result;
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    if (identifier != null)
      return super.execute(currentRecord, context);

    Object result = executeSubquery(context);

    // Apply modifier if present (e.g., (SELECT ...).name or (SELECT ...)[0])
    if (modifier != null)
      result = modifier.execute(currentRecord, result, context);

    return result;
  }

  private Object executeSubquery(final CommandContext context) {
    final ResultSet rs = statement.execute(context.getDatabase(), context.getInputParameters(), context);

    // Always keep Result objects so that modifiers (field access) work correctly.
    // For example: (SELECT name FROM doc).name should extract the name field from each Result.
    // If we extract values early, modifiers can't access fields.
    // Always return the list to match JavaCC behavior
    // Note: This is used in INSERT SET field = (SELECT ...) contexts where
    // the list is expected even for single results
    // Under both heap limits, like the LET of the same subquery (issue #10080), and closed whatever happens
    return OperationHeapLimit.materializeForExpression(context, "subquery expression", rs);
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("(");
    statement.toString(params, builder);
    builder.append(")");
    if (modifier != null)
      modifier.toString(params, builder);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("(").append(statement.toString()).append(")");
    if (modifier != null)
      sb.append(modifier);
    return sb.toString();
  }

  /**
   * The grammar's {@code LPAREN statement RPAREN modifier*} alternative carries a modifier tail of its own, and this
   * node renders its own parentheses, so a pair of parentheses WRITTEN around it is the redundant kind that an
   * unaliased projection must not be renamed by (issue #7896).
   */
  @Override
  protected boolean carriesItsOwnModifierTail() {
    return true;
  }

  @Override
  public SubqueryExpression copy() {
    final SubqueryExpression copy = new SubqueryExpression(statement);
    copy.identifier = identifier == null ? null : identifier.copy();
    if (modifier != null)
      copy.setModifier(modifier.copy());
    return copy;
  }
}
