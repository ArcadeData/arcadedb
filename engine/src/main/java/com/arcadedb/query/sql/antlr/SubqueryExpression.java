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
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.BaseIdentifier;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.SubQueryCollector;

import java.util.Map;

/**
 * Expression wrapper that executes a SELECT subquery when evaluated.
 * Used for left-side subqueries in IN conditions: (SELECT ...) IN collection
 */
public class SubqueryExpression extends BaseExpression {
  private final SelectStatement statement;
  // THE LET VARIABLE THE PLANNER MOVED THE STATEMENT INTO (ISSUE #13179), NULL WHILE IT RUNS IN PLACE. KEPT APART FROM
  // THE INHERITED IDENTIFIER SO THE NODE STILL RENDERS, NAMES ITS PROJECTION COLUMN AND PLANS AS THE SUBQUERY IT IS
  private       BaseIdentifier  liftedValue;

  public SubqueryExpression(final SelectStatement statement) {
    this.statement = statement;
  }

  public SelectStatement getStatement() {
    return statement;
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    Object result = liftedValue != null ?
        liftedValue.execute(currentRecord != null ? currentRecord.getRecord() : null, context) :
        executeSubquery(context);

    // Apply modifier if present (e.g., (SELECT ...).name or (SELECT ...)[0])
    if (modifier != null)
      result = modifier.execute(currentRecord, result, context);

    return result;
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    Object result = liftedValue != null ? liftedValue.execute(currentRecord, context) : executeSubquery(context);

    // Apply modifier if present (e.g., (SELECT ...).name or (SELECT ...)[0])
    if (modifier != null)
      result = modifier.execute(currentRecord, result, context);

    return result;
  }

  /**
   * Hands the statement to the planner, which computes it in a LET: once per execution, or once per row when it refers to
   * {@code $parent}, exactly as the {@code IN (SELECT ...)} form is lifted. Left in place, an uncorrelated subquery read by
   * {@code =}, {@code <}, {@code CONTAINS}, a projection... ran again for every row evaluated (issue #13179). The node cannot
   * replace itself in its parent, so it keeps the statement and reads the LET variable instead, applying its modifier to it as
   * it applies it to the rows it would have produced: both are the materialized list of the subquery's results.
   */
  @Override
  public void extractSubQueries(final SubQueryCollector collector) {
    if (liftedValue == null)
      liftedValue = new BaseIdentifier(collector.addStatement(statement));
  }

  /**
   * A subquery inside the expression of a LET is left in place: the planner adds the LET of a {@code $parent} subquery after
   * every per-record LET, so the expression would read it before it is computed, and the LET of an uncorrelated one before the
   * LETs its statement may read.
   */
  @Override
  public void extractSubQueries(final Identifier letAlias, final SubQueryCollector collector) {
    // NOTHING TO LIFT
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
  protected Object[] getIdentityElements() {
    return new Object[] { statement, liftedValue, modifier };
  }

  @Override
  public SubqueryExpression copy() {
    final SubqueryExpression copy = new SubqueryExpression(statement);
    if (modifier != null)
      copy.setModifier(modifier.copy());
    if (liftedValue != null)
      copy.liftedValue = liftedValue.copy();
    return copy;
  }
}
