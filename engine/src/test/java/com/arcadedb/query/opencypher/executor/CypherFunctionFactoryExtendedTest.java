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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Extended unit tests for CypherFunctionFactory.
 * Tests factory methods, function availability checks, and error handling.
 */
class CypherFunctionFactoryExtendedTest {
  private Database database;
  private CypherFunctionFactory factory;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-cypher-factory-extended").create();
    factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ========== hasFunction Tests ==========

  @Test
  void shouldRecognizeCypherSpecificFunctions() {
    assertThat(factory.hasFunction("id")).isTrue();
    assertThat(factory.hasFunction("labels")).isTrue();
    assertThat(factory.hasFunction("type")).isTrue();
    assertThat(factory.hasFunction("keys")).isTrue();
    assertThat(factory.hasFunction("properties")).isTrue();
    assertThat(factory.hasFunction("startnode")).isTrue();
    assertThat(factory.hasFunction("endnode")).isTrue();
  }

  @Test
  void shouldRecognizeMathFunctions() {
    assertThat(factory.hasFunction("abs")).isTrue();
    // Most math functions are SQL functions, test Cypher-specific ones
    assertThat(factory.hasFunction("rand")).isTrue();
    assertThat(factory.hasFunction("sign")).isTrue();
    assertThat(factory.hasFunction("randomuuid")).isTrue();
  }

  @Test
  void shouldRecognizeAggregationFunctions() {
    assertThat(factory.hasFunction("count")).isTrue();
    assertThat(factory.hasFunction("sum")).isTrue();
    assertThat(factory.hasFunction("avg")).isTrue();
    assertThat(factory.hasFunction("min")).isTrue();
    assertThat(factory.hasFunction("max")).isTrue();
    assertThat(factory.hasFunction("collect")).isTrue();
  }

  @Test
  void shouldRecognizeStringFunctions() {
    // toupper/tolower might not be in SQL factory, test the Cypher-specific ones
    assertThat(factory.hasFunction("left")).isTrue();
    assertThat(factory.hasFunction("right")).isTrue();
    assertThat(factory.hasFunction("reverse")).isTrue();
    assertThat(factory.hasFunction("split")).isTrue();
    assertThat(factory.hasFunction("substring")).isTrue();
  }

  @Test
  void shouldRecognizeListFunctions() {
    assertThat(factory.hasFunction("size")).isTrue();
    assertThat(factory.hasFunction("head")).isTrue();
    assertThat(factory.hasFunction("tail")).isTrue();
    assertThat(factory.hasFunction("last")).isTrue();
    assertThat(factory.hasFunction("range")).isTrue();
  }

  @Test
  void shouldRecognizeTypeConversionFunctions() {
    assertThat(factory.hasFunction("tostring")).isTrue();
    assertThat(factory.hasFunction("tointeger")).isTrue();
    assertThat(factory.hasFunction("tofloat")).isTrue();
    assertThat(factory.hasFunction("toboolean")).isTrue();
  }

  @Test
  void shouldRecognizeTemporalFunctions() {
    assertThat(factory.hasFunction("date")).isTrue();
    assertThat(factory.hasFunction("localtime")).isTrue();
    assertThat(factory.hasFunction("time")).isTrue();
    assertThat(factory.hasFunction("localdatetime")).isTrue();
    assertThat(factory.hasFunction("datetime")).isTrue();
    assertThat(factory.hasFunction("duration")).isTrue();
  }

  @Test
  void shouldRecognizeSQLPrefixedFunctions() {
    assertThat(factory.hasFunction("sql.count")).isTrue();
    assertThat(factory.hasFunction("sql.sum")).isTrue();
    assertThat(factory.hasFunction("sql.max")).isTrue();
  }

  @Test
  void shouldNotRecognizeUnknownFunctions() {
    assertThat(factory.hasFunction("unknownfunction")).isFalse();
    assertThat(factory.hasFunction("notafunction")).isFalse();
    assertThat(factory.hasFunction("")).isFalse();
  }

  @Test
  void shouldHandleCaseInsensitiveFunctionNames() {
    assertThat(factory.hasFunction("ABS")).isTrue();
    assertThat(factory.hasFunction("Abs")).isTrue();
    assertThat(factory.hasFunction("COUNT")).isTrue();
    assertThat(factory.hasFunction("Count")).isTrue();
  }

  // ========== getFunctionExecutor Tests ==========

  @Test
  void shouldReturnExecutorForCypherSpecificFunctions() {
    final StatelessFunction idFunc = factory.getFunctionExecutor("id");
    assertThat(idFunc).isNotNull();

    final StatelessFunction labelsFunc = factory.getFunctionExecutor("labels");
    assertThat(labelsFunc).isNotNull();

    final StatelessFunction coalesceFunc = factory.getFunctionExecutor("coalesce");
    assertThat(coalesceFunc).isNotNull();
  }

  @Test
  void shouldReturnExecutorForMappedSQLFunctions() {
    final StatelessFunction absFunc = factory.getFunctionExecutor("abs");
    assertThat(absFunc).isNotNull();

    final StatelessFunction sumFunc = factory.getFunctionExecutor("sum");
    assertThat(absFunc).isNotNull();
  }

  @Test
  void shouldThrowForUnknownFunction() {
    assertThatThrownBy(() -> factory.getFunctionExecutor("unknownfunction"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Unknown function");
  }

  @Test
  void shouldThrowForUnknownSQLFunction() {
    assertThatThrownBy(() -> factory.getFunctionExecutor("sql.unknownfunction"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Unknown SQL function");
  }

  @Test
  void shouldHandleDistinctAggregation() {
    final StatelessFunction collectFunc = factory.getFunctionExecutor("collect", false);
    assertThat(collectFunc).isNotNull();

    final StatelessFunction collectDistinctFunc = factory.getFunctionExecutor("collect", true);
    assertThat(collectDistinctFunc).isNotNull();

    // Distinct version should be different instance
    assertThat(collectFunc).isNotSameAs(collectDistinctFunc);
  }

  @Test
  void shouldReturnSQLFunctionFactory() {
    assertThat(factory.getSQLFunctionFactory()).isNotNull();
    assertThat(factory.getSQLFunctionFactory()).isSameAs(DefaultSQLFunctionFactory.getInstance());
  }

  // ========== Integration Tests with Database ==========

  @Test
  void shouldExecuteRandFunction() {
    final var result = database.query("opencypher", "RETURN rand() AS result");
    assertThat(result.hasNext()).isTrue();
    final var value = result.next().<Number>getProperty("result");
    assertThat(value.doubleValue()).isBetween(0.0, 1.0);
  }

  @Test
  void shouldExecuteRandomUuidFunction() {
    final var result = database.query("opencypher", "RETURN randomuuid() AS result");
    assertThat(result.hasNext()).isTrue();
    final var value = result.next().<String>getProperty("result");
    assertThat(value).isNotNull();
    assertThat(value).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  }

  @Test
  void shouldExecuteSignFunction() {
    final var positiveResult = database.query("opencypher", "RETURN sign(5) AS result");
    assertThat(positiveResult.hasNext()).isTrue();
    assertThat(positiveResult.next().<Number>getProperty("result").longValue()).isEqualTo(1L);

    final var negativeResult = database.query("opencypher", "RETURN sign(-5) AS result");
    assertThat(negativeResult.hasNext()).isTrue();
    assertThat(negativeResult.next().<Number>getProperty("result").longValue()).isEqualTo(-1L);

    final var zeroResult = database.query("opencypher", "RETURN sign(0) AS result");
    assertThat(zeroResult.hasNext()).isTrue();
    assertThat(zeroResult.next().<Number>getProperty("result").longValue()).isEqualTo(0L);
  }

  @Test
  void shouldExecuteToStringFunction() {
    final var result = database.query("opencypher", "RETURN toString(42) AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("result")).isEqualTo("42");
  }

  @Test
  void shouldExecuteToIntegerFunction() {
    final var result = database.query("opencypher", "RETURN toInteger('42') AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("result").longValue()).isEqualTo(42L);
  }

  @Test
  void shouldExecuteToFloatFunction() {
    final var result = database.query("opencypher", "RETURN toFloat('3.14') AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("result").doubleValue()).isCloseTo(3.14, org.assertj.core.data.Offset.offset(0.001));
  }

  @Test
  void shouldExecuteToBooleanFunction() {
    final var trueResult = database.query("opencypher", "RETURN toBoolean('true') AS result");
    assertThat(trueResult.hasNext()).isTrue();
    assertThat(trueResult.next().<Boolean>getProperty("result")).isTrue();

    final var falseResult = database.query("opencypher", "RETURN toBoolean('false') AS result");
    assertThat(falseResult.hasNext()).isTrue();
    assertThat(falseResult.next().<Boolean>getProperty("result")).isFalse();
  }

  @Test
  void shouldExecuteLeftFunction() {
    final var result = database.query("opencypher", "RETURN left('hello', 3) AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("result")).isEqualTo("hel");
  }

  @Test
  void shouldExecuteRightFunction() {
    final var result = database.query("opencypher", "RETURN right('hello', 3) AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("result")).isEqualTo("llo");
  }

  @Test
  void shouldExecuteReverseFunction() {
    final var result = database.query("opencypher", "RETURN reverse('hello') AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("result")).isEqualTo("olleh");
  }

  @Test
  void shouldExecuteSplitFunction() {
    final var result = database.query("opencypher", "RETURN split('a,b,c', ',') AS result");
    assertThat(result.hasNext()).isTrue();
    final Object resultValue = result.next().getProperty("result");
    assertThat(resultValue).isNotNull();
  }

  @Test
  void shouldExecuteSubstringFunction() {
    final var result = database.query("opencypher", "RETURN substring('hello', 1, 3) AS result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("result")).isEqualTo("ell");
  }

  @Test
  void shouldExecuteSizeFunction() {
    final var result = database.query("opencypher", "RETURN size([1, 2, 3, 4, 5]) AS result");
    assertThat(result.hasNext()).isTrue();
    final Number resultValue = result.next().getProperty("result");
    assertThat(resultValue.longValue()).isEqualTo(5L);
  }
}
