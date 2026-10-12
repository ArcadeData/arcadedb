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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9794: a declaration defines the text form of its values, so the same pattern has to find what it stored through
 * that form, or MERGE is not idempotent. The coercion that does it (#9695) covered one direction only, a non-text written
 * into a property declared STRING: a text written into a property declared INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, DATE or
 * DATETIME was converted and stored, and then no spelling of the same pattern found it, so a second MERGE created a
 * duplicate, or failed on a UNIQUE index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9794DeclaredTypePatternTest extends TestHelper {
  private static final String[] DECLARED = { "STRING", "BYTE", "SHORT", "INTEGER", "LONG", "FLOAT", "DOUBLE", "DECIMAL", "BOOLEAN",
      "DATE", "DATETIME", "DATETIME_MICROS" };

  // A text, and a literal of every other Cypher type: a cell whose write the declaration refuses is skipped
  private static final String[] LITERALS = { "'5'", "'1.5'", "'true'", "'2024-01-02'", "'2024-01-02T10:00:00'", "5", "1.5", "true",
      "date('2024-01-02')", "localdatetime('2024-01-02T10:00:00')" };

  private static final String[] SPELLINGS = { //
      "MATCH (n:%s {p: %s}) RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE n.p = %s RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE %2$s = n.p RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE n['p'] = %s RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE properties(n).p = %s RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE n.p IN [%s] RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE CASE n.p WHEN %s THEN true ELSE false END RETURN count(n) AS c", //
      "MATCH (n:%s) WHERE NOT (n.p <> %s) RETURN count(n) AS c", //
      "UNWIND [%2$s] AS v MATCH (n:%1$s {p: v}) RETURN count(n) AS c", //
      "UNWIND [%2$s] AS v MATCH (n:%1$s) WHERE n.p = v RETURN count(n) AS c" };

  @Test
  void aRepeatedMergeFindsWhatTheFirstStoredThroughTheTextForm() {
    assertEveryCellIsFoundAgain(false);
  }

  @Test
  void anIndexedPropertyFindsItToo() {
    assertEveryCellIsFoundAgain(true);
  }

  @Test
  void aUniqueIndexNoLongerTurnsTheRepeatedMergeIntoAFailure() {
    database.command("sql", "CREATE VERTEX TYPE U");
    database.command("sql", "CREATE PROPERTY U.p INTEGER");
    database.command("sql", "CREATE INDEX ON U (p) UNIQUE");

    database.transaction(() -> database.command("opencypher", "MERGE (:U {p: '5'})").close());
    database.transaction(() -> database.command("opencypher", "MERGE (:U {p: '5'})").close());

    assertThat(count("MATCH (n:U) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:U {p: '5'}) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:U) WHERE n.p = '5' RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void aJoinOnATextFindsTheTypedValue() {
    final Map<String, List<String>> texts = new LinkedHashMap<>();
    texts.put("INTEGER", List.of("1", "2", "3"));
    texts.put("DOUBLE", List.of("1.5", "2.5", "3.5"));
    texts.put("BOOLEAN", List.of("true", "false"));
    texts.put("DATE", List.of("2024-01-02", "2024-01-03", "2024-01-04"));

    int type = 0;
    for (final Map.Entry<String, List<String>> entry : texts.entrySet()) {
      for (final boolean indexed : new boolean[] { false, true }) {
        final String target = "J" + type++;
        final String source = target + "S";
        final List<String> values = entry.getValue();
        database.command("sql", "CREATE VERTEX TYPE " + target);
        database.command("sql", "CREATE PROPERTY " + target + ".p " + entry.getKey());
        if (indexed)
          database.command("sql", "CREATE INDEX ON " + target + " (p) NOTUNIQUE");
        database.command("sql", "CREATE VERTEX TYPE " + source);
        database.transaction(() -> {
          // many targets and few sources, so the planner seeks the index once per source row
          for (int i = 0; i < 300; i++)
            database.command("opencypher", "CREATE (:" + target + " {p: '" + values.get(i % values.size()) + "'})").close();
          for (final String value : values)
            database.command("opencypher", "CREATE (:" + source + " {s: '" + value + "'})").close();
        });

        final String as = entry.getKey() + (indexed ? " indexed" : "");
        final String join = "MATCH (a:" + source + "), (n:" + target + ") WHERE n.p = a.s RETURN count(n) AS c";
        // the join seeks the index with the key a write of the text stores (a temporal key is not sought, it is scanned)
        if (indexed && !entry.getKey().equals("DATE"))
          assertThat(plan(join)).as(as).contains("IndexNestedLoopJoin");
        assertThat(count(join)).as(as).isEqualTo(300);
        assertThat(count("MATCH (a:" + source + ") MATCH (n:" + target + " {p: a.s}) RETURN count(n) AS c")).as(as).isEqualTo(300);
        assertThat(count("MATCH (a:" + source + ") OPTIONAL MATCH (n:" + target + " {p: a.s}) RETURN count(n) AS c")).as(as)
            .isEqualTo(300);
      }
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  @Test
  void aSchemalessPropertyKeepsTheCypherTypingOfEquality() {
    database.command("sql", "CREATE VERTEX TYPE F");
    database.transaction(() -> database.command("opencypher", "CREATE (:F {p: 5}), (:F {p: '5'})").close());

    // with no declaration nothing says the text stands for the number: 5 = '5' is false in Cypher
    assertThat(count("MATCH (n:F {p: '5'}) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:F) WHERE n.p = 5 RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:F) WHERE n.p <> '5' RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void twoValuesOfTheSameKindKeepTheCypherTyping() {
    database.command("sql", "CREATE VERTEX TYPE I");
    database.command("sql", "CREATE PROPERTY I.p INTEGER");
    database.command("sql", "CREATE VERTEX TYPE IX");
    database.command("sql", "CREATE PROPERTY IX.p INTEGER");
    database.command("sql", "CREATE INDEX ON IX (p) NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:I {p: 1}), (:IX {p: 1})").close());

    // only the text form is converted: a fraction equals no integer, with or without an index
    for (final String label : List.of("I", "IX")) {
      assertThat(count("MATCH (n:" + label + " {p: 1.5}) RETURN count(n) AS c")).as(label).isZero();
      assertThat(count("MATCH (n:" + label + ") WHERE n.p = 1.5 RETURN count(n) AS c")).as(label).isZero();
      assertThat(count("MATCH (n:" + label + ") WHERE n.p = 1.0 RETURN count(n) AS c")).as(label).isEqualTo(1);
    }
  }

  @Test
  void aTextTheDeclarationCannotReadMatchesNothing() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.p INTEGER");
    database.transaction(() -> database.command("opencypher", "CREATE (:N {p: 5})").close());

    assertThat(count("MATCH (n:N {p: 'five'}) RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:N) WHERE n.p = 'five' RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:N) WHERE n.p <> 'five' RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:N) WHERE n.p IN ['five', '6'] RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n:N) WHERE n.p IN ['five', '5'] RETURN count(n) AS c")).isEqualTo(1);
  }

  /**
   * A text matches exactly what a write of it stores: the declaration's own reading of the text, no more lenient. An integral
   * declaration reads a sign and digits ({@code '05'} and {@code '+5'} are 5, {@code '5.7'}, {@code ' 5'} and {@code '1e2'}
   * are refused by the write and match nothing); a DOUBLE reads every spelling {@code Double.parseDouble} does.
   */
  @Test
  void aNonCanonicalNumericTextMatchesWhatItsWriteStores() {
    database.command("sql", "CREATE VERTEX TYPE I");
    database.command("sql", "CREATE PROPERTY I.p INTEGER");
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.p DOUBLE");
    database.transaction(() -> database.command("opencypher", "CREATE (:I {p: 5}), (:D {p: 2.5})").close());

    for (final String text : List.of("'05'", "'+5'")) {
      assertThat(count("MATCH (n:I {p: " + text + "}) RETURN count(n) AS c")).as(text).isEqualTo(1);
      assertThat(count("MATCH (n:I) WHERE n.p = " + text + " RETURN count(n) AS c")).as(text).isEqualTo(1);
      database.transaction(() -> database.command("opencypher", "MERGE (:I {p: " + text + "})").close());
    }
    assertThat(count("MATCH (n:I) RETURN count(n) AS c")).isEqualTo(1);

    assertThat(count("MATCH (n:I {p: '0000000000005'}) RETURN count(n) AS c")).isEqualTo(1);
    for (final String text : List.of("'5.7'", "' 5'", "'1e2'", "''", "'five'", "'99999999999'")) {
      assertThat(count("MATCH (n:I {p: " + text + "}) RETURN count(n) AS c")).as(text).isZero();
      assertThat(count("MATCH (n:I) WHERE n.p = " + text + " RETURN count(n) AS c")).as(text).isZero();
      assertThat(count("MATCH (n:I) WHERE n.p <> " + text + " RETURN count(n) AS c")).as(text).isEqualTo(1);
    }

    for (final String text : List.of("'2.5'", "'2.50'", "' 2.5'", "'25e-1'"))
      assertThat(count("MATCH (n:D {p: " + text + "}) RETURN count(n) AS c")).as(text).isEqualTo(1);
    assertThat(count("MATCH (n:D {p: 'x2.5'}) RETURN count(n) AS c")).isZero();
  }

  /**
   * The shape check in front of the conversion only saves the conversion's exception: for every declared type and text it
   * answers exactly what {@code Type.convert} answers, so it can never refuse a text a write stores (and then miss the record).
   */
  @Test
  void theShapeCheckNeverDisagreesWithTheConversion() {
    final DocumentType type = database.getSchema().createVertexType("S");
    final List<String> texts = List.of("5", "+5", "-5", "005", "0000000000005", "5.7", " 5", "5 ", "1e2", "", "-", "+", "five",
        "127", "128", "-129", "32768", "2147483647", "2147483648", "9223372036854775807", "9223372036854775808",
        "99999999999999999999", "2.5", "2.50", " 2.5", "25e-1", ".5", "-.5", "NaN", "Infinity", "-Infinity", "0x1p3", "1.5d", "x2.5",
        "true", "TRUE", "False", "yes", "1", "0", "2024-01-02", "2024-01-02T10:00:00", "2024-13-45", "foo", "P1D", "10:15:30",
        "1704153600000", "-0", "1e3", "Nope", "Inf", "-NaN", "+Infinity", "+.5", "5.");
    for (final Type declared : List.of(Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.DECIMAL,
        Type.BOOLEAN, Type.DATE, Type.DATETIME, Type.DATETIME_MICROS, Type.LOCAL_TIME, Type.DURATION)) {
      final Property property = type.createProperty("p" + declared.name().toLowerCase(), declared);
      for (final String text : texts) {
        Object written = Type.convertOrNull(database, text, declared.getJavaImplementation(database), property);
        if (written instanceof Double d && d.isNaN() || written instanceof Float f && f.isNaN())
          written = null; // NaN equals nothing, not even itself
        assertThat(InlineProperties.declaredValue(property, database, text)).as(declared + " <- '" + text + "'").isEqualTo(written);
      }
    }
  }

  @Test
  void aTextNoTemporalDeclarationReadsMatchesNothing() {
    database.command("sql", "CREATE VERTEX TYPE E");
    database.command("sql", "CREATE PROPERTY E.at DATETIME");
    database.transaction(() -> database.command("opencypher", "CREATE (:E {at: '2024-01-02T10:00:00'})").close());

    assertThat(count("MATCH (n:E) WHERE n.at = '2024-01-02T10:00:00' RETURN count(n) AS c")).isEqualTo(1);
    for (final String text : List.of("'foo'", "'2024-13-45T99:00:00'", "''")) {
      assertThat(count("MATCH (n:E {at: " + text + "}) RETURN count(n) AS c")).as(text).isZero();
      assertThat(count("MATCH (n:E) WHERE n.at = " + text + " RETURN count(n) AS c")).as(text).isZero();
    }
  }

  @Test
  void aRelationshipPropertyFollowsItsDeclarationToo() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE PROPERTY E.w DOUBLE");
    database.transaction(() -> database.command("opencypher", "CREATE (:V {k: 1}), (:V {k: 2})").close());

    for (int i = 0; i < 2; i++)
      database.transaction(
          () -> database.command("opencypher", "MATCH (a:V {k: 1}), (b:V {k: 2}) MERGE (a)-[:E {w: '2.5'}]->(b)").close());

    assertThat(count("MATCH ()-[r:E]->() RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:E {w: '2.5'}]->() RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:E]->() WHERE r.w = '2.5' RETURN count(r) AS c")).isEqualTo(1);
  }

  /**
   * For every declared type and literal whose write converts through the text form - a text into any declaration, anything
   * into a STRING - or stores the literal as it is, MERGE run twice leaves one node and every spelling of the pattern finds it.
   */
  private void assertEveryCellIsFoundAgain(final boolean indexed) {
    final List<String> failures = new ArrayList<>();
    int cells = 0;
    int type = 0;
    for (final String declared : DECLARED) {
      for (final String literal : LITERALS) {
        if (!convertsThroughTheTextForm(declared, literal) && !isSameType(declared, literal))
          continue;

        final String label = (indexed ? "X" : "T") + type++;
        database.command("sql", "CREATE VERTEX TYPE " + label);
        database.command("sql", "CREATE PROPERTY " + label + ".p " + declared);
        // UNIQUE: a second MERGE that did not find the first node fails on the index instead of duplicating it silently
        if (indexed)
          database.command("sql", "CREATE INDEX ON " + label + " (p) UNIQUE");

        try {
          database.transaction(() -> database.command("opencypher", "MERGE (:" + label + " {p: " + literal + "})").close());
        } catch (final RuntimeException refused) {
          // the declaration cannot store this literal: no pattern stores anything, so there is nothing to find again
          continue;
        }
        cells++;

        try {
          database.transaction(() -> database.command("opencypher", "MERGE (:" + label + " {p: " + literal + "})").close());
        } catch (final RuntimeException e) {
          failures.add(declared + " <- " + literal + ": the second MERGE failed: " + e.getMessage());
        }
        if (count("MATCH (n:" + label + ") RETURN count(n) AS c") != 1)
          failures.add(declared + " <- " + literal + ": the second MERGE created a duplicate");
        // a text no declaration reads matches nothing, under the same plan
        if (count("MATCH (n:" + label + " {p: 'abc'}) RETURN count(n) AS c") != 0)
          failures.add(declared + " <- " + literal + ": 'abc' matched");

        for (final String spelling : SPELLINGS) {
          final String query = spelling.formatted(label, literal);
          if (count(query) != 1)
            failures.add(declared + " <- " + literal + ": " + query);
        }
      }
    }

    assertThat(cells).isGreaterThan(40);
    assertThat(failures).isEmpty();
  }

  private static boolean convertsThroughTheTextForm(final String declared, final String literal) {
    return declared.equals("STRING") != literal.startsWith("'");
  }

  private static boolean isSameType(final String declared, final String literal) {
    return switch (literal) {
      case "5" -> !declared.equals("STRING") && !declared.equals("BOOLEAN") && !declared.startsWith("DATE");
      case "1.5" -> declared.equals("FLOAT") || declared.equals("DOUBLE") || declared.equals("DECIMAL");
      case "true" -> declared.equals("BOOLEAN");
      case "date('2024-01-02')" -> declared.equals("DATE");
      case "localdatetime('2024-01-02T10:00:00')" -> declared.startsWith("DATETIME");
      default -> declared.equals("STRING");
    };
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Object c = rs.next().getProperty("c");
      return c instanceof Number n ? n.longValue() : -1;
    } catch (final RuntimeException e) {
      return -2;
    }
  }
}
