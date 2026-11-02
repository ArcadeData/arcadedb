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
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11051: the SQL function factory stores every name lower-cased, and {@code register} and {@code getFunctionInstance}
 * lower-cased on their way in, but {@code hasFunction}, {@code isSharedInstance}, {@code isBuiltIn} and {@code unregister} used
 * the raw name. A Cypher {@code CALL} gates on {@code hasFunction}, so {@code CALL vector.l2Distance(...)} was refused as an
 * unknown procedure under the very name the function registers itself with, and {@code unregister("Acme.Twice")} removed
 * nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11051FunctionNameCaseTest extends TestHelper {

  /** A scalar function registered under a mixed-case name, the way an application spells its own namespace. */
  public static class Twice extends SQLFunctionAbstract {
    public static final String NAME = "Acme11051.Twice";

    public Twice() {
      super(NAME);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      return ((Number) params[0]).intValue() * 2;
    }

    @Override
    public String getAlias() {
      return "acme11051.doubleIt";
    }

    @Override
    public String getSyntax() {
      return NAME + "(<n>)";
    }
  }

  @Test
  void everyBuiltInAnswersTheSameUnderItsOwnSpellingAndAnyOther() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    final List<String> disagreements = new ArrayList<>();
    for (final String key : new TreeSet<>(factory.getBuiltInFunctionNames())) {
      final SQLFunction function = factory.getFunctionInstance(key);
      assertThat(function).as(key).isNotNull();
      // The canonical spelling is the one the function gives itself; an alias key keeps its own spelling
      final String canonical = function.getName().toLowerCase(Locale.ENGLISH).equals(key) ? function.getName() : key;
      for (final String spelling : new String[] { canonical, key.toUpperCase(Locale.ENGLISH) }) {
        if (!factory.hasFunction(spelling))
          disagreements.add("hasFunction(" + spelling + ")");
        if (!factory.isBuiltIn(spelling))
          disagreements.add("isBuiltIn(" + spelling + ")");
        if (factory.isSharedInstance(spelling) != factory.isSharedInstance(key))
          disagreements.add("isSharedInstance(" + spelling + ")");
        if (factory.getFunctionInstance(spelling) == null)
          disagreements.add("getFunctionInstance(" + spelling + ")");
      }
    }
    assertThat(disagreements).isEmpty();
  }

  @Test
  void cypherCallResolvesCamelCaseBuiltInsUnderTheirCanonicalNames() {
    assertThat(callValue("CALL vector.l2Distance([1,0],[0,1]) YIELD value RETURN value")).isCloseTo(Math.sqrt(2.0), within(1e-6));
    assertThat(callValue("CALL vector.l2distance([1,0],[0,1]) YIELD value RETURN value")).isCloseTo(Math.sqrt(2.0), within(1e-6));
    assertThat(callValue("CALL vector.Magnitude([3,4]) YIELD value RETURN value")).isCloseTo(5.0, within(1e-6));
    assertThat(callValue("CALL vector.Dimension([1,2,3]) YIELD value RETURN value")).isCloseTo(3.0, within(1e-6));
    assertThat(callValue("CALL vector.cosineSimilarity([1,0],[1,0]) YIELD value RETURN value")).isCloseTo(1.0, within(1e-6));

    try (final ResultSet rs = database.query("opencypher", "CALL geo.asText(geo.point(1,2)) YIELD value RETURN value")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("POINT (1 2)");
    }
  }

  @Test
  void unregisterUnderTheRegisteredSpellingRemovesTheFunctionAndItsAlias() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    factory.register(new Twice());
    try {
      assertThat(factory.hasFunction(Twice.NAME)).isTrue();
      assertThat(factory.hasFunction("acme11051.twice")).isTrue();
      assertThat(factory.hasFunction("ACME11051.DOUBLEIT")).isTrue();
      assertThat(factory.isSharedInstance(Twice.NAME)).isTrue();
      assertThat(factory.isBuiltIn(Twice.NAME)).as("registered after construction").isFalse();
      try (final ResultSet rs = database.query("opencypher", "CALL Acme11051.Twice(21) YIELD value RETURN value")) {
        assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(42);
      }
    } finally {
      factory.unregister(Twice.NAME);
      // An instance registration also lands in the process-wide unified registry, which unregister() leaves alone on purpose
      FunctionRegistry.unregister(Twice.NAME);
    }
    assertThat(factory.hasFunction(Twice.NAME)).isFalse();
    assertThat(factory.getFunctionInstance("acme11051.twice")).isNull();
    assertThat(factory.hasFunction("acme11051.doubleit")).as("the alias goes with the function").isFalse();
  }

  @Test
  void unregisterOfAClassRegistrationUnderAnotherCaseRemovesIt() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    factory.register(Twice.NAME, Twice.class);
    try {
      assertThat(factory.hasFunction(Twice.NAME)).isTrue();
      assertThat(factory.isSharedInstance(Twice.NAME)).as("a class registration makes one instance per call").isFalse();
      assertThat(factory.getFunctionInstance(Twice.NAME)).isInstanceOf(Twice.class);
    } finally {
      factory.unregister("ACME11051.TWICE");
    }
    assertThat(factory.hasFunction(Twice.NAME)).isFalse();
    assertThat(factory.hasFunction("acme11051.doubleit")).isFalse();
  }

  private double callValue(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final Result row = rs.next();
      return ((Number) row.getProperty("value")).doubleValue();
    }
  }
}
