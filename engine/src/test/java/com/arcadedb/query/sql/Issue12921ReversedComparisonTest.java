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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12921: a comparison of a typed property with a literal or a parameter answered differently with the
 * literal on the left. {@code d = '2024-06-01 12:00:00'} converted the string to the DATETIME of {@code d}, while
 * {@code '2024-06-01 12:00:00' = d} compared the date-time with the string as text; {@code p = true} and {@code true = p},
 * {@code p < '10'} and {@code '10' > p} disagreed on an INTEGER the same way. The form with the property on the left is the
 * reference: {@code lit op p} must answer the rows {@code p op' lit} answers, {@code op'} being {@code op} mirrored.
 */
class Issue12921ReversedComparisonTest extends TestHelper {
  private static final String[] PROPERTIES = { "i INTEGER", "l LONG", "dbl DOUBLE", "dec DECIMAL", "s STRING", "b BOOLEAN", "d DATE",
      "dt DATETIME", "dtm DATETIME_MICROS", "dtn DATETIME_NANOS" };

  /** The literal as written in the statement and the same value bound as a parameter. */
  private static final Object[][] LITERALS = { { "5", 5 }, { "1.5", 1.5 }, { "'10'", "10" }, { "'a'", "a" }, { "true", true },
      { "'2024-01-01'", "2024-01-01" }, { "'2024-01-01 00:00:00'", "2024-01-01 00:00:00" },
      { "'2024-06-01 12:00:00'", "2024-06-01 12:00:00" } };

  /** Each operator and its mirror: {@code a op b} is {@code b mirror a}. */
  private static final String[][] OPERATORS = { { "=", "=" }, { "<>", "<>" }, { "<", ">" }, { "<=", ">=" }, { ">", "<" },
      { ">=", "<=" } };

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      for (final String typeName : new String[] { "Plain", "Indexed" }) {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".id INTEGER");
        for (final String property : PROPERTIES) {
          database.command("sql", "CREATE PROPERTY " + typeName + "." + property);
          if (typeName.equals("Indexed"))
            database.command("sql", "CREATE INDEX ON " + typeName + " (" + property.split(" ")[0] + ") NOTUNIQUE");
        }
        insert(typeName, 1, 1, 1.5, "1", true, LocalDate.of(2023, 12, 31), LocalDateTime.of(2023, 12, 31, 23, 59, 59));
        insert(typeName, 2, 5, 5.0, "5", false, LocalDate.of(2024, 1, 1), LocalDateTime.of(2024, 1, 1, 0, 0));
        insert(typeName, 3, -3, -3.0, "a", true, LocalDate.of(2024, 6, 1), LocalDateTime.of(2024, 6, 1, 12, 0));
        insert(typeName, 4, 10, 10.0, "10", false, LocalDate.of(2024, 6, 2), LocalDateTime.of(2024, 6, 1, 12, 0, 1));
        insert(typeName, 5, 0, 0.0, "", false, LocalDate.of(1970, 1, 1), LocalDateTime.of(1970, 1, 1, 0, 0));
        database.newDocument(typeName).set("id", 6).save();
      }
    });
  }

  private void insert(final String typeName, final int id, final int number, final double decimal, final String string,
      final boolean bool, final LocalDate date, final LocalDateTime dateTime) {
    final MutableDocument doc = database.newDocument(typeName).set("id", id);
    doc.set("i", number).set("l", (long) number).set("dbl", decimal).set("dec", new BigDecimal(Double.toString(decimal)));
    doc.set("s", string).set("b", bool).set("d", date).set("dt", dateTime).set("dtm", dateTime).set("dtn", dateTime);
    doc.save();
  }

  /**
   * The reference is the property-on-the-left form answered by a scan (type {@code Plain}). The type {@code Indexed} holds the same
   * rows with an index on every property, which the reversed form does not use, so it checks that the scan the planner falls back to
   * answers the same. The reference is not read from {@code Indexed}: an index range lookup answers a few of these cells differently
   * from the scan of the same statement ({@code dt > '10'}, {@code dec > true}), which is not a matter of operand order.
   */
  @Test
  void literalOnTheLeftAnswersLikeTheLiteralOnTheRight() {
    final List<String> mismatches = new ArrayList<>();
    int compared = 0;
    for (final String property : PROPERTIES) {
      final String p = property.split(" ")[0];
      for (final Object[] literal : LITERALS)
        for (final String[] operator : OPERATORS) {
          final Map<String, Object> params = Map.of("v", literal[1]);
          final String reference = rows("SELECT id FROM Plain WHERE " + p + " " + operator[0] + " " + literal[0], null);
          final String referenceParam = rows("SELECT id FROM Plain WHERE " + p + " " + operator[0] + " :v", params);
          for (final String typeName : new String[] { "Plain", "Indexed" }) {
            final String reversed = rows("SELECT id FROM " + typeName + " WHERE " + literal[0] + " " + operator[1] + " " + p, null);
            if (!reference.equals(reversed))
              mismatches.add(typeName + ": " + p + " " + operator[0] + " " + literal[0] + " -> " + reference + " but " + literal[0] + " "
                  + operator[1] + " " + p + " -> " + reversed);

            final String reversedParam = rows("SELECT id FROM " + typeName + " WHERE :v " + operator[1] + " " + p, params);
            if (!referenceParam.equals(reversedParam))
              mismatches.add(typeName + ": " + p + " " + operator[0] + " :v(" + literal[1] + ") -> " + referenceParam + " but :v "
                  + operator[1] + " " + p + " -> " + reversedParam);
            compared += 2;
          }
        }
    }

    assertThat(compared).isEqualTo(2 * PROPERTIES.length * LITERALS.length * OPERATORS.length * 2);
    assertThat(mismatches).as("comparisons whose operand-reversed form answers other rows").isEmpty();
  }

  @Test
  void reportedDateTimeCases() {
    for (final String typeName : new String[] { "Plain", "Indexed" }) {
      assertThat(rows("SELECT id FROM " + typeName + " WHERE '2024-06-01 12:00:00' = dt", null)).isEqualTo("[3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE '2024-01-01 00:00:00' < dt", null)).isEqualTo("[3, 4]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE '2024-01-01 00:00:00' >= dt", null)).isEqualTo("[1, 2, 5]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE :p = dt", Map.of("p", "2024-06-01 12:00:00"))).isEqualTo("[3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE true = i", null)).isEqualTo(rows("SELECT id FROM " + typeName + " WHERE i = true", null));
      assertThat(rows("SELECT id FROM " + typeName + " WHERE '10' > i", null)).isEqualTo("[1, 2, 3, 5]");
    }
  }

  /**
   * The mirror is taken for whatever is on the right of a literal, not only a declared property: a schemaless property holds the
   * class it was written with and a function call answers a value of its own, and both convert the literal as they do on the left.
   */
  @Test
  void literalOnTheLeftOfASchemalessPropertyOrAFunctionCall() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Loose");
      database.newDocument("Loose").set("id", 1).set("x", 1).save();
      database.newDocument("Loose").set("id", 2).set("x", 5).save();
      database.newDocument("Loose").set("id", 3).set("x", -3).save();
      database.newDocument("Loose").set("id", 4).set("x", 10).save();
      database.newDocument("Loose").set("id", 5).set("x", 0).save();
    });

    for (final String[] operator : OPERATORS)
      for (final String value : new String[] { "x", "abs(x)", "x + 0" }) {
        assertThat(rows("SELECT id FROM Loose WHERE '10' " + operator[1] + " " + value, null))
            .as("'10' %s %s", operator[1], value)
            .isEqualTo(rows("SELECT id FROM Loose WHERE " + value + " " + operator[0] + " '10'", null));
        assertThat(rows("SELECT id FROM Loose WHERE :v " + operator[1] + " " + value, Map.of("v", "10")))
            .as(":v %s %s", operator[1], value)
            .isEqualTo(rows("SELECT id FROM Loose WHERE " + value + " " + operator[0] + " :v", Map.of("v", "10")));
      }

    // compared as numbers, not as text: "5" sorts after "10"
    assertThat(rows("SELECT id FROM Loose WHERE '10' > x", null)).isEqualTo("[1, 2, 3, 5]");
    assertThat(rows("SELECT id FROM Loose WHERE '10' > abs(x)", null)).isEqualTo("[1, 2, 3, 5]");
  }

  /**
   * {@code <=>} is equality that tests the null too, so two scalars compare as {@code =} does whichever side the literal is on. A
   * list keeps the meaning of the side it is written on: on the left any item may match, on the right it is one value.
   */
  @Test
  void nullSafeEqualityWithTheLiteralOnTheLeft() {
    for (final String typeName : new String[] { "Plain", "Indexed" }) {
      assertThat(rows("SELECT id FROM " + typeName + " WHERE '2024-06-01 12:00:00' <=> dt", null)).isEqualTo("[3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE :p <=> dt", Map.of("p", "2024-06-01 12:00:00"))).isEqualTo("[3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE true <=> i", null)).isEqualTo(
          rows("SELECT id FROM " + typeName + " WHERE i <=> true", null));
      assertThat(rows("SELECT id FROM " + typeName + " WHERE '10' <=> i", null)).isEqualTo("[4]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE null <=> i", null)).isEqualTo("[6]");
    }

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Tagged");
      database.newDocument("Tagged").set("id", 1).set("tags", List.of("a", "b")).save();
      database.newDocument("Tagged").set("id", 2).set("tags", List.of("c")).save();
      database.newDocument("Tagged").set("id", 3).set("tags", List.of("a")).save();
    });
    // "=" and "<>" read a list alike on either side, so the mirror leaves them answering what the property-first form answers
    for (final String operator : new String[] { "=", "<>" }) {
      assertThat(rows("SELECT id FROM Tagged WHERE 'a' " + operator + " tags", null)).as("'a' %s tags", operator)
          .isEqualTo(rows("SELECT id FROM Tagged WHERE tags " + operator + " 'a'", null));
      assertThat(rows("SELECT id FROM Tagged WHERE ['a'] " + operator + " tags", null)).as("['a'] %s tags", operator)
          .isEqualTo(rows("SELECT id FROM Tagged WHERE tags " + operator + " ['a']", null));
    }
    // the list on the left: any of its items
    assertThat(rows("SELECT id FROM Tagged WHERE tags <=> 'a'", null)).isEqualTo("[1, 3]");
    // the list on the right is one value, which the string is not
    assertThat(rows("SELECT id FROM Tagged WHERE 'a' <=> tags", null)).isEqualTo("[]");
  }

  /** The ids the query answers, sorted, or the class of the exception it raised. */
  private String rows(final String sql, final Map<String, Object> params) {
    final TreeSet<Integer> ids = new TreeSet<>();
    try (final ResultSet result = params == null ? database.query("sql", sql) : database.query("sql", sql, params)) {
      while (result.hasNext())
        ids.add(result.next().<Integer>getProperty("id"));
    } catch (final RuntimeException e) {
      return "error " + e.getClass().getSimpleName();
    }
    return ids.toString();
  }
}
