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
package com.arcadedb.function;

import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10523: the openCypher {@code length()} function counted the UTF-16 code units of a string, so {@code length('𝔸X')} answered 3
 * where {@code size('𝔸X')}, {@code char_length('𝔸X')} and the SQL {@code length()} method answer 2. Every conversion of the string family
 * had to find its own call sites and missed one each time (#8998, #10248, #10378, #10417), so besides the function itself this test
 * enumerates the registries instead of naming functions: every registered Cypher function and SQL method that takes one string and
 * answers a number or a boolean has to answer the same for a character outside the BMP as for a BMP character, because the encoding of
 * a character is not something a query can see.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10523StringLengthCountsCharactersTest {
  // MATHEMATICAL DOUBLE-STRUCK CAPITAL A, two UTF-16 code units
  private static final String ASTRAL = new String(Character.toChars(0x1D538));
  // a letter of the BMP with the same character count
  private static final String BMP    = "À";
  // the two functions whose job is to expose the encoding: the code point value of the first character (À is 192, 𝔸 is 120120) and the
  // number of UTF-8 bytes. Every other function must not.
  private static final Set<String> ANSWERS_THE_ENCODING = Set.of("text.code", "text.byteCount");
  /** What a call that threw answers, so that a call which threw is told from one that answered null. */
  private static final Object      THREW                = new Object();

  @Test
  void lengthOfAStringCountsCharactersNotCodeUnits() {
    final CypherFunctionFactory factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());
    final StatelessFunction length = factory.getFunctionExecutor("length");
    final StatelessFunction size = factory.getFunctionExecutor("size");
    final StatelessFunction charLength = factory.getFunctionExecutor("char_length");

    assertThat(length.execute(new Object[] { ASTRAL + "X" }, new BasicCommandContext())).isEqualTo(2L);
    assertThat(size.execute(new Object[] { ASTRAL + "X" }, new BasicCommandContext())).isEqualTo(2L);
    assertThat(charLength.execute(new Object[] { ASTRAL + "X" }, new BasicCommandContext())).isEqualTo(2L);
    // two characters outside the BMP, four code units
    assertThat(length.execute(new Object[] { ASTRAL + ASTRAL }, new BasicCommandContext())).isEqualTo(2L);
    assertThat(length.execute(new Object[] { "" }, new BasicCommandContext())).isEqualTo(0L);
    assertThat(length.execute(new Object[] { "abc" }, new BasicCommandContext())).isEqualTo(3L);
  }

  @Test
  void path_lengthAliasAnswersLikeLength() {
    // `length` and `path_length` are the same instance
    final StatelessFunction pathLength = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance()).getFunctionExecutor("path_length");
    assertThat(pathLength.execute(new Object[] { ASTRAL + "X" }, new BasicCommandContext())).isEqualTo(2L);
  }

  @Test
  void noCypherFunctionOrSqlMethodCountsTheEncodingOfACharacter() {
    final List<String> differing = new ArrayList<>();

    final List<StatelessFunction> functions = new ArrayList<>(CypherFunctionRegistry.getAllFunctions());
    final CypherFunctionFactory cypherFactory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());
    for (final String name : CypherBuiltinFunctions.getNames())
      functions.add(cypherFactory.getFunctionExecutor(name));

    final List<String> checked = new ArrayList<>();
    for (final StatelessFunction function : functions) {
      if (function.getMinArgs() > 1 || function.getMaxArgs() < 1)
        continue;
      final Object astral = answerOf(function, ASTRAL + "X");
      final Object bmp = answerOf(function, BMP + "X");
      if ((astral == THREW) != (bmp == THREW)) {
        // a function that refuses one of the two strings and answers the other depends on the encoding as much as one that counts it
        differing.add("cypher " + function.getName() + "(): " + (astral == THREW ? "fails" : "answers") + " for the astral string, "
            + (bmp == THREW ? "fails" : "answers") + " for the BMP one");
        continue;
      }
      if (!isScalarCount(astral) || !isScalarCount(bmp))
        continue;
      checked.add(function.getName());
      if (!astral.equals(bmp) && !ANSWERS_THE_ENCODING.contains(function.getName()))
        differing.add("cypher " + function.getName() + "(): " + astral + " for the astral string, " + bmp + " for the BMP one");
    }
    // the sweep is not vacuous: it reaches the three functions of the report
    assertThat(checked).contains("length", "size", "char_length");

    final DefaultSQLMethodFactory factory = DefaultSQLMethodFactory.getInstance();
    final List<String> checkedMethods = new ArrayList<>();
    for (final String name : factory.getBuiltInMethodNames()) {
      final SQLMethod method = factory.createMethod(name);
      if (method.getMinParams() > 0)
        continue;
      final Object astral = answerOf(method, ASTRAL + "X");
      final Object bmp = answerOf(method, BMP + "X");
      if ((astral == THREW) != (bmp == THREW)) {
        differing.add("sql ." + name + "(): " + (astral == THREW ? "fails" : "answers") + " for the astral string, "
            + (bmp == THREW ? "fails" : "answers") + " for the BMP one");
        continue;
      }
      if (!isScalarCount(astral) || !isScalarCount(bmp))
        continue;
      checkedMethods.add(name);
      if (!astral.equals(bmp))
        differing.add("sql ." + name + "(): " + astral + " for the astral string, " + bmp + " for the BMP one");
    }
    assertThat(checkedMethods).contains("length", "size");

    assertThat(differing).as("functions and methods whose answer depends on how a character is encoded").isEmpty();
  }

  private static boolean isScalarCount(final Object answer) {
    return answer instanceof Number || answer instanceof Boolean;
  }

  private static Object answerOf(final StatelessFunction function, final String input) {
    try {
      return function.execute(new Object[] { input }, new BasicCommandContext());
    } catch (final RuntimeException e) {
      return THREW;
    }
  }

  private static Object answerOf(final SQLMethod method, final String input) {
    try {
      return method.execute(input, null, new BasicCommandContext(), new Object[0]);
    } catch (final RuntimeException e) {
      return THREW;
    }
  }
}
