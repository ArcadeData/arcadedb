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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.literal.LiteralParameterizer;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9925: reading a property of a scanned record repeated, for every row, string work that depends only on the query
 * text - unescaping the identifier, testing it for the Postgres wire's {@code $N} shape, and probing the parameter map for a
 * colon-prefixed spelling of a generated literal parameter. These tests pin that the work is now done once per identifier, and
 * that every answer it feeds stays what it was. The last ones pin two bugs found on the way: a caller could replace a literal of
 * the query text by binding {@code ":__lit_..."}, and a {@code $N} too large for an int failed the query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9925PerRowIdentifierWorkTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG");
    database.command("sql", "CREATE PROPERTY V.name STRING");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("V").set("a", (long) i).set("name", "n" + i).set("a`b\\c", "escaped" + i).save();
    });
  }

  @Test
  void anIdentifierUnescapesItsNameOnce() {
    final Identifier plain = new Identifier("l_quantity");
    assertThat(plain.getStringValue()).isEqualTo("l_quantity");
    assertThat(plain.getStringValue()).isSameAs(plain.getValue());

    // a name that needs escaping: unescaping it builds a new String, so a second read returning the same instance proves the
    // plain name is kept rather than rebuilt per call
    final Identifier escaped = new Identifier("a`b\\c");
    assertThat(escaped.getValue()).isEqualTo("a\\`b\\\\c");
    assertThat(escaped.getStringValue()).isEqualTo("a`b\\c");
    assertThat(escaped.getStringValue()).isSameAs(escaped.getStringValue());

    final Identifier quoted = Identifier.quoted("`x\\`y`");
    assertThat(quoted.getValue()).isEqualTo("x\\`y");
    assertThat(quoted.getStringValue()).isEqualTo("x`y");
    assertThat(quoted.getStringValue()).isSameAs(quoted.getStringValue());
  }

  @Test
  void anIdentifierFollowsEveryWriteOfItsName() {
    final Identifier identifier = new Identifier("first");
    assertThat(identifier.getStringValue()).isEqualTo("first");

    identifier.setStringValue("se`cond");
    assertThat(identifier.getStringValue()).isEqualTo("se`cond");
    assertThat(identifier.positionalParameterKey()).isNull();

    identifier.setQuotedStringValue("`$3`");
    assertThat(identifier.getStringValue()).isEqualTo("$3");
    assertThat(identifier.positionalParameterKey()).isEqualTo("2");

    identifier.setStringValue(null);
    assertThat(identifier.getStringValue()).isNull();
    assertThat(identifier.positionalParameterKey()).isNull();

    final Identifier copy = new Identifier(new Identifier("a`b"), true);
    assertThat(copy.getStringValue()).isEqualTo("a`b");
    assertThat(new Identifier(new Identifier("$7"), false).positionalParameterKey()).isEqualTo("6");
  }

  @Test
  void anIdentifierKnowsWhetherItIsAPostgresPositionalParameter() {
    assertThat(new Identifier("$1").positionalParameterKey()).isEqualTo("0");
    assertThat(new Identifier("$12").positionalParameterKey()).isEqualTo("11");
    assertThat(new Identifier("$" + Integer.MAX_VALUE).positionalParameterKey()).isEqualTo(String.valueOf(Integer.MAX_VALUE - 1));

    assertThat(new Identifier("$").positionalParameterKey()).isNull();
    assertThat(new Identifier("$0").positionalParameterKey()).as("Postgres positions start at 1").isNull();
    assertThat(new Identifier("$1a").positionalParameterKey()).isNull();
    assertThat(new Identifier("$parent").positionalParameterKey()).isNull();
    assertThat(new Identifier("name").positionalParameterKey()).isNull();
    assertThat(new Identifier("1").positionalParameterKey()).isNull();
    assertThat(new Identifier("$99999999999").positionalParameterKey()).as("beyond any position a caller can bind").isNull();
  }

  @Test
  void aPostgresPositionalParameterStillReadsItsBoundValue() {
    final Map<String, Object> params = new HashMap<>();
    params.put("0", 4L);
    try (final ResultSet rs = database.query("sql", "SELECT name FROM V WHERE a = $1", params)) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n4");
      assertThat(rs.hasNext()).isFalse();
    }

    // with nothing bound at its position, $2 is not a parameter: it reads as an (unset) variable
    try (final ResultSet rs = database.query("sql", "SELECT $2 AS v, name FROM V WHERE a = $1", params)) {
      assertThat(rs.next().<Object>getProperty("v")).isNull();
    }
  }

  @Test
  void aPositionalShapeTooLargeForAnIntIsNotAParameter() {
    // used to fail with a NumberFormatException as soon as any parameter was bound
    final Map<String, Object> params = new HashMap<>();
    params.put("0", 4L);
    try (final ResultSet rs = database.query("sql", "SELECT $99999999999 AS v FROM V WHERE a = $1", params)) {
      assertThat(rs.next().<Object>getProperty("v")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void dollarNamesStillResolveAfterTheFirstCharacterGate() {
    // $parent, reached through the Result overload of a sub-query's WHERE
    try (final ResultSet rs = database.query("sql",
        "SELECT a, (SELECT count(*) AS c FROM V WHERE a = $parent.$current.a)[0].c AS same FROM V WHERE a = 2")) {
      assertThat(rs.next().<Long>getProperty("same")).isEqualTo(1L);
    }

    // a LET variable carried as record metadata
    try (final ResultSet rs = database.query("sql", "SELECT $x AS v FROM V LET $x = a * 2 WHERE a = 3")) {
      assertThat(rs.next().<Number>getProperty("v").longValue()).isEqualTo(6L);
    }
  }

  @Test
  void aPlainPropertyIsNotShadowedByAVariableOfTheSameName() {
    ((DatabaseInternal) database).setGlobalVariable("name", "shadow");
    try {
      try (final ResultSet rs = database.query("sql", "SELECT name, $name AS g FROM V WHERE a = 1")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("n1");
        assertThat(row.<String>getProperty("g")).isEqualTo("shadow");
      }
    } finally {
      ((DatabaseInternal) database).setGlobalVariable("name", null);
    }
  }

  @Test
  void anIdentifierWithoutANameEvaluatesToNull() {
    final SuffixIdentifier suffix = new SuffixIdentifier(new Identifier((String) null));
    assertThat(suffix.execute((Result) null, null)).isNull();
  }

  @Test
  void anEscapedPropertyNameStillReadsItsValue() {
    try (final ResultSet rs = database.query("sql", "SELECT `a\\`b\\\\c` AS v FROM V WHERE a = 3")) {
      assertThat(rs.next().<String>getProperty("v")).isEqualTo("escaped3");
    }
  }

  @Test
  void aCallerCannotReplaceALiteralOfTheQueryText() {
    // the same shape with other values, so the literal is extracted into a generated parameter
    for (int i = 0; i < 3; i++)
      try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE name = 'n" + i + "'")) {
        assertThat(rs.next().<Long>getProperty("a")).isEqualTo((long) i);
      }

    final String text = "SELECT a FROM V WHERE name = 'n5'";
    final Lookup<Statement> lookup = ((DatabaseInternal) database).getStatementCache().getParameterized(text);
    assertThat(lookup.parameters()).as("the literal is extracted").isNotNull().isNotEmpty();

    // binding the colon-prefixed spelling of every generated name must not change what the text says
    final Map<String, Object> params = new HashMap<>();
    for (final String name : lookup.parameters().keySet()) {
      assertThat(name).startsWith(LiteralParameterizer.PARAMETER_PREFIX);
      params.put(":" + name, "n7");
    }
    try (final ResultSet rs = database.query("sql", text, params)) {
      assertThat(rs.next().<Long>getProperty("a")).isEqualTo(5L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aGeneratedParameterReadsOnlyItsPlainName() {
    final NamedParameter generated = new NamedParameter();
    generated.paramName = LiteralParameterizer.PARAMETER_PREFIX + "s0";
    generated.paramNumber = 2;

    final Map<String, Object> params = new HashMap<>();
    params.put(":" + generated.paramName, "caller");
    params.put(generated.paramName, "literal");
    assertThat(generated.getValue(params)).isEqualTo("literal");
    assertThat(generated.copy().getValue(params)).isEqualTo("literal");

    // a name renamed into or out of the generated namespace follows the rule of its current name
    generated.paramName = "p";
    params.put(":p", "prefixed");
    params.put("p", "plain");
    assertThat(generated.getValue(params)).isEqualTo("prefixed");
    generated.paramName = LiteralParameterizer.PARAMETER_PREFIX + "s0";
    assertThat(generated.getValue(params)).isEqualTo("literal");
  }

  @Test
  void aUserNamedParameterStillPrefersItsColonPrefixedKey() {
    final Map<String, Object> params = new HashMap<>();
    params.put(":name", "n2");
    params.put("name", "n3");
    try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE name = :name", params)) {
      assertThat(rs.next().<Long>getProperty("a")).isEqualTo(2L);
    }
  }
}
