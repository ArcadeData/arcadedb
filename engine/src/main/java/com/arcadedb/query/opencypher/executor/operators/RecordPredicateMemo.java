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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.executor.RowIndependentValues;
import com.arcadedb.query.opencypher.executor.steps.MatchNodeStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.RidIntHashMap;

/**
 * What a filter already answered for a record, so a predicate on one record is evaluated once per record instead of once
 * per row. The far end of a multi-hop MATCH is the same few thousand vertices on hundreds of thousands of rows, and
 * {@code x.age > 41} read and decoded the property again on every one of them.
 * <p>
 * Only a predicate whose answer the record alone decides is remembered ({@link #variableOf}): comparisons of a property
 * of ONE variable with a value no row changes, and AND, OR and NOT of those. The answer is kept by the address of the
 * record the variable is bound to on the row; a row that binds it to anything else is evaluated as before.
 * <p>
 * The gates are the ones of {@link ExpandedVertexCache}, for the same reasons: a read-only statement on a thread that reads
 * the committed state (in a transaction that wrote, two rows can hold different images of a record), nothing kept for the first
 * {@link #WARM_UP_ROWS} rows, dropped when fewer than one row in four is answered from it over {@link #REVIEW_EVERY}
 * rows, and at most {@link #MAX_ENTRIES} answers, two primitives each. Not thread-safe: it belongs to one result set.
 */
final class RecordPredicateMemo {
  static final int WARM_UP_ROWS = 256;
  static final int REVIEW_EVERY = 4_096;
  static final int MAX_ENTRIES  = 65_536;

  private static final int ANSWER_FALSE = 0;
  private static final int ANSWER_TRUE  = 1;
  private static final int ANSWER_NULL  = 2;

  private final BooleanExpression predicate;
  private final String            variable;
  private       RidIntHashMap     answers;
  private       int               warmUpRows;
  private       int               windowRows;
  private       int               windowHits;
  private       boolean           armed;
  private       boolean           off;

  private RecordPredicateMemo(final BooleanExpression predicate, final String variable) {
    this.predicate = predicate;
    this.variable = variable;
  }

  /**
   * The variable whose record alone decides {@code predicate}, or null when something else does too: another variable, a
   * function, a pattern, a subquery, or a shape this does not know. Asked once, when the operator is planned.
   */
  static String variableOf(final BooleanExpression predicate) {
    if (predicate instanceof ComparisonExpression comparison) {
      final String left = variableOfSide(comparison.getLeft(), comparison.getRight());
      return left != null ? left : variableOfSide(comparison.getRight(), comparison.getLeft());
    }
    if (predicate instanceof LogicalExpression logical) {
      final String left = variableOf(logical.getLeft());
      if (left == null || logical.getRight() == null)
        // NOT has one operand
        return logical.getOperator() == LogicalExpression.Operator.NOT ? left : null;
      return left.equals(variableOf(logical.getRight())) ? left : null;
    }
    return null;
  }

  private static String variableOfSide(final Expression property, final Expression value) {
    return property instanceof PropertyAccessExpression access && RowIndependentValues.isRowIndependent(value) ?
        access.getVariableName() :
        null;
  }

  /** The memo of one execution of a filter on {@code variable}, or null when the statement is not read-only. */
  static RecordPredicateMemo of(final BooleanExpression predicate, final String variable, final CommandContext context) {
    if (variable == null || !Boolean.TRUE.equals(context.getVariable(MatchNodeStep.READ_ONLY_STATEMENT_KEY)))
      return null;
    return new RecordPredicateMemo(predicate, variable);
  }

  /** Called before each batch of rows is filtered: the memo starts, and keeps, answering only while the thread reads the committed state. */
  void startBatch(final Database database) {
    if (off || warmUpRows < WARM_UP_ROWS)
      return;
    if (!ExpandedVertexCache.readsCommittedStateOnly(database)) {
      drop();
      return;
    }
    armed = true;
  }

  /** What the predicate answers for {@code row}: TRUE, FALSE or null. */
  Object evaluate(final Result row, final CommandContext context) {
    if (!armed) {
      if (!off)
        ++warmUpRows;
      return predicate.evaluateTernary(row, context);
    }

    final Object bound = row.getProperty(variable);
    final RID rid = bound instanceof Document document ? document.getIdentity() : null;
    if (rid == null || rid.getPosition() < 0)
      // NOT A STORED RECORD: A MAP, A PATH, A RECORD NOT SAVED YET, A LIGHTWEIGHT EDGE (WHOSE ADDRESS NAMES NO RECORD)
      return predicate.evaluateTernary(row, context);

    ++windowRows;
    final int known = answers == null ? -1 : answers.get(rid.getBucketId(), rid.getPosition(), -1);
    if (known >= 0) {
      ++windowHits;
      return known == ANSWER_NULL ? null : known == ANSWER_TRUE;
    }

    final Object answer = predicate.evaluateTernary(row, context);

    if (windowRows >= REVIEW_EVERY) {
      if ((long) windowHits * 4 < windowRows) {
        drop();
        return answer;
      }
      windowRows = 0;
      windowHits = 0;
    }

    if (answer == null || answer instanceof Boolean) {
      if (answers == null)
        answers = new RidIntHashMap(256);
      if (answers.size() < MAX_ENTRIES)
        answers.put(rid.getBucketId(), rid.getPosition(),
            answer == null ? ANSWER_NULL : (Boolean) answer ? ANSWER_TRUE : ANSWER_FALSE);
    }
    return answer;
  }

  private void drop() {
    off = true;
    armed = false;
    answers = null;
  }

  boolean isAnswering() {
    return armed;
  }

  int size() {
    return answers == null ? 0 : answers.size();
  }
}
