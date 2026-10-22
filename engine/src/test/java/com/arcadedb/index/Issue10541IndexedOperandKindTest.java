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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10541: a lookup on an indexed property with a value of another kind than the property can hold answers what the same
 * query answers without the index.
 * <p>
 * The index converts the operand to its declared key type, and for a kind the type cannot hold the conversion either leaves the
 * operand as it was or answers {@code null}, neither of which is a key. The first reached the key comparison, which cast it
 * ({@code ClassCastException} for a BOOLEAN or DECIMAL property, {@code IllegalArgumentException} "Comparison between type N and N not
 * supported" for a DATE) and the scan answered no rows; the second was read as "no bound", so {@code p > ?} with a Boolean, a List, a
 * Map or a LocalDate on an INTEGER or DOUBLE property returned every row. An {@code IN} whose right side is not a list is refused by the
 * openCypher scan and was answered with no rows through the index.
 * <p>
 * The guarantee is the one differential the issue measures, over every property type, operand kind and predicate shape of both
 * languages, on one indexed type and one identical type without the index: the two answer the same, rows or the refusal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10541IndexedOperandKindTest extends TestHelper {

  /** One row per type, holding what each property type holds. */
  private static final Map<String, Object> STORED = new LinkedHashMap<>();

  /** What a caller can bind: every kind a property type can or cannot hold. */
  private static final Map<String, Object> OPERANDS = new LinkedHashMap<>();

  static {
    STORED.put("BOOLEAN", true);
    STORED.put("INTEGER", 7);
    STORED.put("LONG", 7L);
    STORED.put("DOUBLE", 1.5);
    STORED.put("DECIMAL", new BigDecimal("1.5"));
    STORED.put("STRING", "abc");
    STORED.put("DATE", LocalDate.of(2020, 1, 1));
    STORED.put("DATETIME", LocalDateTime.of(2020, 1, 1, 0, 0));

    OPERANDS.put("String", "zzz");
    OPERANDS.put("NumericString", "5");
    OPERANDS.put("Integer", 7);
    OPERANDS.put("Long", 7L);
    OPERANDS.put("Double", 1.5);
    OPERANDS.put("BigDecimal", new BigDecimal("1.5"));
    OPERANDS.put("Boolean", true);
    OPERANDS.put("LocalDate", LocalDate.of(2020, 1, 1));
    OPERANDS.put("LocalDateTime", LocalDateTime.of(2020, 1, 1, 0, 0));
    // Holding what the INTEGER and LONG rows hold, so that an operand split into its items, or into its values, would find a row
    OPERANDS.put("List", List.of(7));
    OPERANDS.put("Map", Map.of("a", 7));
  }

  /**
   * The operand kinds each property type cannot hold. For an equality or an IN they match no row, and the index has to answer so, as the
   * scan does. For a range the scan orders some of them across kinds by what they convert to (a LocalDate against a DECIMAL by its epoch
   * number, a Boolean against it as 1), a leniency the index cannot reproduce because its keys have one order, one kind: there the index
   * may decline (no rows) but must neither throw nor answer a row the scan does not.
   */
  private static final Map<String, List<String>> FOREIGN = Map.of(
      "BOOLEAN", List.of("LocalDate", "LocalDateTime", "List", "Map"),
      "INTEGER", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
      "LONG", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
      "DOUBLE", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
      "DECIMAL", List.of("Boolean", "LocalDate", "LocalDateTime", "List", "Map"),
      "STRING", List.of("List", "Map"),
      "DATE", List.of("Boolean", "List", "Map", "NumericString"),
      "DATETIME", List.of("Boolean", "List", "Map", "NumericString"));

  /** SQL shapes, {@code :x} the operand and {@code :v} what the property holds, so an IN can hold one element that matches. */
  private static final List<String> SQL_EQUALITIES = List.of("p = :x", "p IN [:x]", "p IN [:x, :v]", "p IN :x");
  private static final List<String> SQL_RANGES     = List.of("p > :x", "p >= :x", "p < :x", "p <= :x");

  /** openCypher shapes. */
  private static final List<String> CYPHER_EQUALITIES = List.of("n.p = $x", "n.p IN [$x]", "n.p IN [$x, $v]", "n.p IN $x");
  private static final List<String> CYPHER_RANGES     = List.of("n.p > $x", "n.p >= $x", "n.p < $x", "n.p <= $x");

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "NOTUNIQUE", "UNIQUE", "UNIQUE_HASH" })
  void everyOperandKindAnswersWithTheIndexAsWithoutIt(final String indexKind) {
    createTypes(indexKind);

    final List<String> differences = new ArrayList<>();
    for (final Map.Entry<String, Object> stored : STORED.entrySet())
      for (final Map.Entry<String, Object> operand : OPERANDS.entrySet()) {
        final Map<String, Object> parameters = Map.of("x", operand.getValue(), "v", stored.getValue());
        final boolean foreign = FOREIGN.get(stored.getKey()).contains(operand.getKey());
        for (final String predicate : SQL_EQUALITIES)
          compare(differences, "sql", stored.getKey(), predicate, operand.getKey(), parameters,
              "SELECT id FROM %s WHERE " + predicate, false);
        for (final String predicate : SQL_RANGES)
          compare(differences, "sql", stored.getKey(), predicate, operand.getKey(), parameters,
              "SELECT id FROM %s WHERE " + predicate, foreign);
        for (final String predicate : CYPHER_EQUALITIES)
          compare(differences, "opencypher", stored.getKey(), predicate, operand.getKey(), parameters,
              "MATCH (n:%s) WHERE " + predicate + " RETURN n.id AS id", false);
        for (final String predicate : CYPHER_RANGES)
          compare(differences, "opencypher", stored.getKey(), predicate, operand.getKey(), parameters,
              "MATCH (n:%s) WHERE " + predicate + " RETURN n.id AS id", foreign);
      }

    assertThat(differences).as("cases where the indexed type and the one without the index answer differently on %s "
        + "(language | property | predicate | operand | with the index | without the index)", indexKind).isEmpty();
  }

  /** The differential is worth something only if the indexed type is read through its index: a plain equality is. */
  @Test
  void theIndexedTypesAreReadThroughTheirIndex() {
    createTypes("NOTUNIQUE");

    for (final String property : STORED.keySet()) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT id FROM I_" + property + " WHERE p = :v", Map.of("v", STORED.get(property)))) {
        assertThat(rs.next().<String>getProperty("executionPlanAsString")).as("SQL plan on %s", property).contains("FETCH FROM INDEX");
      }
      try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (n:I_" + property + ") WHERE n.p = $v RETURN n.id AS id",
          Map.of("v", STORED.get(property)))) {
        rs.stream().count();
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).as("Cypher plan on %s", property).contains("NodeIndexSeek");
      }
    }
  }

  /** A list element that matches is found whatever the other elements are: the elements of another kind are skipped, not fatal. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "NOTUNIQUE", "UNIQUE_HASH" })
  void anInListFindsTheElementThatMatchesBesideOnesOfAnotherKind(final String indexKind) {
    createTypes(indexKind);

    for (final Map.Entry<String, Object> stored : STORED.entrySet())
      for (final Map.Entry<String, Object> operand : OPERANDS.entrySet()) {
        final Map<String, Object> parameters = Map.of("x", operand.getValue(), "v", stored.getValue());
        assertThat(run("sql", "SELECT id FROM I_" + stored.getKey() + " WHERE p IN [:x, :v]", parameters))
            .as("SQL IN [%s, stored] on %s", operand.getKey(), stored.getKey()).isEqualTo("rows [1]");
        assertThat(run("opencypher", "MATCH (n:I_" + stored.getKey() + ") WHERE n.p IN [$x, $v] RETURN n.id AS id", parameters))
            .as("Cypher IN [%s, stored] on %s", operand.getKey(), stored.getKey()).isEqualTo("rows [1]");
      }
  }

  private void createTypes(final String indexKind) {
    for (final Map.Entry<String, Object> stored : STORED.entrySet()) {
      final String property = stored.getKey();
      for (final String prefix : new String[] { "I_", "N_" }) {
        final String type = prefix + property;
        database.command("sql", "CREATE VERTEX TYPE " + type).close();
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
        database.command("sql", "CREATE PROPERTY " + type + ".p " + property).close();
      }
      database.command("sql", "CREATE INDEX ON I_" + property + " (p) " + indexKind).close();
      database.transaction(() -> {
        for (final String prefix : new String[] { "I_", "N_" })
          database.newVertex(prefix + property).set("id", 1).set("p", stored.getValue()).save();
      });
    }
  }

  /** {@code mayDecline}: the index may answer no rows where the scan orders the operand across kinds, see {@link #FOREIGN}. */
  private void compare(final List<String> differences, final String language, final String property, final String predicate,
      final String operand, final Map<String, Object> parameters, final String template, final boolean mayDecline) {
    final String indexed = run(language, String.format(template, "I_" + property), parameters);
    final String scanned = run(language, String.format(template, "N_" + property), parameters);
    if (!indexed.equals(scanned) && !(mayDecline && indexed.equals("rows []")))
      differences.add(language + " | " + property + " | " + predicate + " | " + operand + " | " + indexed + " | " + scanned);
  }

  /** The ids the query answers, sorted, or the class of the exception at the bottom of what it threw. */
  private String run(final String language, final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query(language, query, parameters)) {
      final List<String> ids = new ArrayList<>();
      while (rs.hasNext())
        ids.add(String.valueOf(rs.next().<Object>getProperty("id")));
      Collections.sort(ids);
      return "rows " + ids;
    } catch (final Throwable e) {
      Throwable root = e;
      while (root.getCause() != null)
        root = root.getCause();
      return root.getClass().getSimpleName();
    }
  }
}
