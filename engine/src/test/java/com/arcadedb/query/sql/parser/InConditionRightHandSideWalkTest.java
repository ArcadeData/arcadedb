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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether the right-hand side of an {@code IN} is constant is a property of the parse tree, and deciding it walks every item of
 * the list. It used to be decided again for every row, so a residual {@code IN} / {@code NOT IN} over a literal list cost rows x
 * listSize before a comparison was made (#12702). The walk is counted here rather than timed: once per execution for a literal
 * list (whose membership probe lives in the execution's context), and no more than once at all for a list that is not constant.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class InConditionRightHandSideWalkTest extends TestHelper {
  private static final int ROWS       = 1_000;
  private static final int EXECUTIONS = 3;

  /** Wraps the parsed right-hand side as its only operand, so it executes the same, and counts the literal walks asked of it. */
  private static final class CountingRightHandSide extends MathExpression {
    private int literalWalks;

    private CountingRightHandSide(final MathExpression wrapped) {
      childExpressions.add(wrapped);
    }

    @Override
    public boolean isLiteral(final boolean allowInputParameters) {
      ++literalWalks;
      return super.isLiteral(allowInputParameters);
    }
  }

  @Test
  void aLiteralListIsWalkedOncePerExecutionNotOncePerRow() {
    final CountingRightHandSide right = new CountingRightHandSide(null);
    final InCondition in = inCondition("v IN [1, 3, 5, 7]", right);

    for (int e = 0; e < EXECUTIONS; e++) {
      final BasicCommandContext context = newContext();
      for (int i = 0; i < ROWS; i++)
        assertThat(in.evaluate(row(i), context)).as("row %d", i).isEqualTo(i == 1 || i == 3 || i == 5 || i == 7);
    }

    assertThat(right.literalWalks).isEqualTo(EXECUTIONS);
  }

  @Test
  void aNotInOverALiteralListIsWalkedOncePerExecutionNotOncePerRow() {
    final CountingRightHandSide right = new CountingRightHandSide(null);
    final InCondition in = inCondition("v NOT IN [1, 3, 5, 7]", right);

    for (int e = 0; e < EXECUTIONS; e++) {
      final BasicCommandContext context = newContext();
      for (int i = 0; i < ROWS; i++)
        assertThat(in.evaluate(row(i), context)).as("row %d", i).isEqualTo(!(i == 1 || i == 3 || i == 5 || i == 7));
    }

    assertThat(right.literalWalks).isEqualTo(EXECUTIONS);
  }

  @Test
  void aListThatIsNotConstantIsWalkedOnceAndStillEvaluatedPerRow() {
    final CountingRightHandSide right = new CountingRightHandSide(null);
    // `w` is a property of the row, so the list is not constant and has to be evaluated for every row
    final InCondition in = inCondition("v IN [w, -1]", right);

    for (int e = 0; e < EXECUTIONS; e++) {
      final BasicCommandContext context = newContext();
      for (int i = 0; i < ROWS; i++) {
        final ResultInternal row = row(i);
        row.setProperty("w", i % 2 == 0 ? i : i + 1);
        assertThat(in.evaluate(row, context)).as("row %d", i).isEqualTo(i % 2 == 0);
      }
    }

    assertThat(right.literalWalks).isEqualTo(1);
  }

  @Test
  void aReplacedRightHandSideIsJudgedAgain() {
    final InCondition in = inCondition("v IN [w]", new CountingRightHandSide(null));
    final ResultInternal row = row(2);
    row.setProperty("w", 2);
    assertThat(in.evaluate(row, newContext())).isTrue();

    // The node remembers which expression it found not to be constant: a literal put in its place is a different one
    final CountingRightHandSide literal = new CountingRightHandSide(parseRightHandSide("v IN [5]"));
    in.setRightMathExpression(literal);
    final BasicCommandContext context = newContext();
    assertThat(in.evaluate(row(5), context)).isTrue();
    assertThat(in.evaluate(row(2), context)).isFalse();
    assertThat(literal.literalWalks).isEqualTo(1);
  }

  private InCondition inCondition(final String condition, final CountingRightHandSide right) {
    final InCondition in = findInCondition(new SQLAntlrParser(database).parseCondition(condition).getBaseExpression());
    assertThat(in).as("the IN condition of %s", condition).isNotNull();
    if (right.childExpressions.getFirst() == null)
      right.childExpressions.set(0, in.getRightMathExpression());
    in.setRightMathExpression(right);
    return in;
  }

  private MathExpression parseRightHandSide(final String condition) {
    return findInCondition(new SQLAntlrParser(database).parseCondition(condition).getBaseExpression()).getRightMathExpression();
  }

  private static InCondition findInCondition(final BooleanExpression expression) {
    if (expression instanceof InCondition in)
      return in;
    if (expression instanceof NotBlock not)
      return findInCondition(not.getSub());
    if (expression instanceof OrBlock or)
      for (final BooleanExpression sub : or.getSubBlocks()) {
        final InCondition found = findInCondition(sub);
        if (found != null)
          return found;
      }
    if (expression instanceof AndBlock and)
      for (final BooleanExpression sub : and.getSubBlocks()) {
        final InCondition found = findInCondition(sub);
        if (found != null)
          return found;
      }
    return null;
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private static ResultInternal row(final int v) {
    final ResultInternal row = new ResultInternal();
    row.setProperty("v", v);
    return row;
  }
}
