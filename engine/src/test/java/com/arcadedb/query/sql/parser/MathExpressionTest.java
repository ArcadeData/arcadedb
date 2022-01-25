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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * Created by luigidellaquila on 02/07/15.
 */
public class MathExpressionTest {

  @Test
  public void testTypes() {

    MathExpression expr = new MathExpression(-1);

    MathExpression.Operator[] basicOps = new MathExpression.Operator[] { MathExpression.Operator.PLUS, MathExpression.Operator.MINUS,
        MathExpression.Operator.STAR, MathExpression.Operator.SLASH, MathExpression.Operator.REM };

    for (MathExpression.Operator op : basicOps) {
      Assertions.assertEquals(op.apply(1, 1).getClass(), Integer.class);

      Assertions.assertEquals(op.apply((short) 1, (short) 1).getClass(), Integer.class);

      Assertions.assertEquals(op.apply(1l, 1l).getClass(), Long.class);
      Assertions.assertEquals(op.apply(1f, 1f).getClass(), Float.class);
      Assertions.assertEquals(op.apply(1d, 1d).getClass(), Double.class);
      Assertions.assertEquals(op.apply(BigDecimal.ONE, BigDecimal.ONE).getClass(), BigDecimal.class);

      Assertions.assertEquals(op.apply(1l, 1).getClass(), Long.class);
      Assertions.assertEquals(op.apply(1f, 1).getClass(), Float.class);
      Assertions.assertEquals(op.apply(1d, 1).getClass(), Double.class);
      Assertions.assertEquals(op.apply(BigDecimal.ONE, 1).getClass(), BigDecimal.class);

      Assertions.assertEquals(op.apply(1, 1l).getClass(), Long.class);
      Assertions.assertEquals(op.apply(1, 1f).getClass(), Float.class);
      Assertions.assertEquals(op.apply(1, 1d).getClass(), Double.class);
      Assertions.assertEquals(op.apply(1, BigDecimal.ONE).getClass(), BigDecimal.class);
    }

    Assertions.assertEquals(MathExpression.Operator.PLUS.apply(Integer.MAX_VALUE, 1).getClass(), Long.class);
    Assertions.assertEquals(MathExpression.Operator.MINUS.apply(Integer.MIN_VALUE, 1).getClass(), Long.class);
  }

  @Test
  public void testPriority() {
    MathExpression exp = new MathExpression(-1);
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

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(208, result);
  }

  @Test
  public void testPriority2() {
    MathExpression exp = new MathExpression(-1);
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

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(16, result);
  }

  @Test
  public void testPriority3() {
    MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(3));
    exp.operators.add(MathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(2, result);
  }

  @Test
  public void testPriority4() {
    MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(3));
    exp.operators.add(MathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(MathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(3, result);
  }

  @Test
  public void testAnd() {
    MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(5));
    exp.operators.add(MathExpression.Operator.BIT_AND);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(1, result);
  }

  @Test
  public void testAnd2() {
    MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(5));
    exp.operators.add(MathExpression.Operator.BIT_AND);
    exp.childExpressions.add(integer(4));

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(4, result);
  }

  @Test
  public void testOr() {
    MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(integer(4));
    exp.operators.add(MathExpression.Operator.BIT_OR);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((Result) null, null);
    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(5, result);
  }

  private MathExpression integer(Number i) {
    BaseExpression exp = new BaseExpression(-1);
    PInteger integer = new PInteger(-1);
    integer.setValue(i);
    exp.number = integer;
    return exp;
  }

  private MathExpression str(String value) {
    final BaseExpression exp = new BaseExpression(-1);
    exp.string = "'" + value + "'";
    return exp;
  }

  private MathExpression nullExpr() {
    return new BaseExpression(-1);
  }

  private MathExpression list(Number... values) {
    BaseExpression exp = new BaseExpression(-1);
    exp.identifier = new BaseIdentifier(-1);
    exp.identifier.levelZero = new LevelZeroIdentifier(-1);
    PCollection coll = new PCollection(-1);
    exp.identifier.levelZero.collection = coll;

    for (Number val : values) {
      Expression sub = new Expression(-1);
      sub.mathExpression = integer(val);
      coll.expressions.add(sub);
    }
    return exp;
  }

  @Test
  public void testNullCoalescing() {
    testNullCoalescingGeneric(integer(20), integer(15), 20);
    testNullCoalescingGeneric(nullExpr(), integer(14), 14);
    testNullCoalescingGeneric(str("32"), nullExpr(), "32");
    testNullCoalescingGeneric(str("2"), integer(5), "2");
    testNullCoalescingGeneric(nullExpr(), str("3"), "3");
  }

  private void testNullCoalescingGeneric(MathExpression left, MathExpression right, Object expected) {
    MathExpression exp = new MathExpression(-1);
    exp.childExpressions.add(left);
    exp.operators.add(MathExpression.Operator.NULL_COALESCING);
    exp.childExpressions.add(right);

    Object result = exp.execute((Result) null, null);
    //    Assertions.assertTrue(result instanceof Integer);
    Assertions.assertEquals(expected, result);
  }
}
