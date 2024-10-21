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

import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by luigidellaquila on 02/07/15.
 */
public class MathExpressionTest {

  @Test
  public void testTypes() {

    final MathExpression expr = new MathExpression(-1);

    final MathExpression.Operator[] basicOps = new MathExpression.Operator[] { MathExpression.Operator.PLUS, MathExpression.Operator.MINUS,
        MathExpression.Operator.STAR, MathExpression.Operator.SLASH, MathExpression.Operator.REM };

    for (final MathExpression.Operator op : basicOps) {
      assertThat(op.apply(1, 1).getClass()).isEqualTo(Integer.class);

      assertThat(op.apply((short) 1, (short) 1).getClass()).isEqualTo(Integer.class);

      assertThat(op.apply(1l, 1l).getClass()).isEqualTo(Long.class);
      assertThat(op.apply(1f, 1f).getClass()).isEqualTo(Float.class);
      assertThat(op.apply(1d, 1d).getClass()).isEqualTo(Double.class);
      assertThat(op.apply(BigDecimal.ONE, BigDecimal.ONE).getClass()).isEqualTo(BigDecimal.class);

      assertThat(op.apply(1l, 1).getClass()).isEqualTo(Long.class);
      assertThat(op.apply(1f, 1).getClass()).isEqualTo(Float.class);
      assertThat(op.apply(1d, 1).getClass()).isEqualTo(Double.class);
      assertThat(op.apply(BigDecimal.ONE, 1).getClass()).isEqualTo(BigDecimal.class);

      assertThat(op.apply(1, 1l).getClass()).isEqualTo(Long.class);
      assertThat(op.apply(1, 1f).getClass()).isEqualTo(Float.class);
      assertThat(op.apply(1, 1d).getClass()).isEqualTo(Double.class);
      assertThat(op.apply(1, BigDecimal.ONE).getClass()).isEqualTo(BigDecimal.class);
    }

    assertThat(MathExpression.Operator.PLUS.apply(Integer.MAX_VALUE, 1).getClass()).isEqualTo(Long.class);
    assertThat(MathExpression.Operator.MINUS.apply(Integer.MIN_VALUE, 1).getClass()).isEqualTo(Long.class);
  }

  @Test
  public void testPriority() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(10));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(5));
    exp.operators.add(MathExpression.Operator.STAR);
    exp.childExpressions.add(integer(8));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(2));
    exp.operators.add(MathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(1));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(208);
  }

  @Test
  public void testPriority2() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(2));
    exp.operators.add(MathExpression.Operator.STAR);
    exp.childExpressions.add(integer(3));
    exp.operators.add(MathExpression.Operator.STAR);
    exp.childExpressions.add(integer(4));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(8));
    exp.operators.add(MathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(2));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.MINUS);
    exp.childExpressions.add(integer(3));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(1));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(16);
  }

  @Test
  public void testPriority3() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(3));
    exp.operators.add(MathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(2);
  }

  @Test
  public void testPriority4() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(3));
    exp.operators.add(MathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(1));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(3);
  }

  @Test
  public void testAnd() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(5));
    exp.operators.add(MathExpression.Operator.BIT_AND);
    exp.childExpressions.add(integer(1));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(1);
  }

  @Test
  public void testAnd2() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(5));
    exp.operators.add(MathExpression.Operator.BIT_AND);
    exp.childExpressions.add(integer(4));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(4);
  }

  @Test
  public void testDivide() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(20));
    exp.operators.add(MathExpression.Operator.SLASH);
    exp.childExpressions.add(integer(4));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void testDivideByNull() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(20));
    exp.operators.add(MathExpression.Operator.SLASH);
    exp.childExpressions.add(nullValue());

    final Object result = exp.execute((Result) null, null);
    assertThat(result).isNull();
  }

  @Test
  public void testOr() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(4));
    exp.operators.add(MathExpression.Operator.BIT_OR);
    exp.childExpressions.add(integer(1));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(5);
  }

  @Test
  public void testNullCoalescing() {
    testNullCoalescingGeneric(integer(20), integer(15), 20);
    testNullCoalescingGeneric(nullExpr(), integer(14), 14);
    testNullCoalescingGeneric(str("32"), nullExpr(), "32");
    testNullCoalescingGeneric(str("2"), integer(5), "2");
    testNullCoalescingGeneric(nullExpr(), str("3"), "3");
  }

  @Test
  public void testAddListOfNumbers() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(list(1, 2, 3));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(5));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof List<?>).isTrue();
    assertThat(((List<Object>) result).get(3)).isEqualTo(5);
  }

  @Test
  public void testAddListOfStrings() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(list("this", "is", "a"));
    exp.operators.add(MathExpression.Operator.PLUS);
    exp.childExpressions.add(str("test"));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof List<?>).isTrue();
    assertThat(((List<Object>) result).get(3)).isEqualTo("test");
  }

  @Test
  public void testRemoveListOfStrings() {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(list("this", "is", "a", "test"));
    exp.operators.add(MathExpression.Operator.MINUS);
    exp.childExpressions.add(str("a"));

    final Object result = exp.execute((Result) null, null);
    assertThat(result instanceof List<?>).isTrue();
    assertThat(((List<?>) result).size()).isEqualTo(3);
    assertThat(((List<Object>) result).contains("a")).isFalse();
  }

  private void testNullCoalescingGeneric(final MathExpression left, final MathExpression right, final Object expected) {
    final MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(left);
    exp.operators.add(MathExpression.Operator.NULL_COALESCING);
    exp.childExpressions.add(right);

    final Object result = exp.execute((Result) null, null);
    //    Assertions.assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(expected);
  }

  private MathExpression integer(final Number i) {
    final BaseExpression exp = new BaseExpression(-1);
    final PInteger integer = new PInteger(-1);
    integer.setValue(i);
    exp.number = integer;
    return exp;
  }

  private BaseExpression nullValue() {
    final BaseExpression exp = new BaseExpression(-1);
    exp.isNull = true;
    return exp;
  }

  private MathExpression str(final String value) {
    final BaseExpression exp = new BaseExpression(-1);
    exp.string = "'" + value + "'";
    return exp;
  }

  private MathExpression nullExpr() {
    return new BaseExpression(-1);
  }

  private MathExpression list(final Number... values) {
    final BaseExpression exp = new BaseExpression(-1);
    exp.identifier = new BaseIdentifier(-1);
    exp.identifier.levelZero = new LevelZeroIdentifier(-1);
    final PCollection coll = new PCollection(-1);
    exp.identifier.levelZero.collection = coll;

    for (final Number val : values) {
      final Expression sub = new Expression(-1);
      sub.mathExpression = integer(val);
      coll.expressions.add(sub);
    }
    return exp;
  }

  private MathExpression list(final String... values) {
    final BaseExpression exp = new BaseExpression(-1);
    exp.identifier = new BaseIdentifier(-1);
    exp.identifier.levelZero = new LevelZeroIdentifier(-1);
    final PCollection coll = new PCollection(-1);
    exp.identifier.levelZero.collection = coll;

    for (final String val : values) {
      final Expression sub = new Expression(-1);
      sub.mathExpression = str(val);
      coll.expressions.add(sub);
    }
    return exp;
  }
}
