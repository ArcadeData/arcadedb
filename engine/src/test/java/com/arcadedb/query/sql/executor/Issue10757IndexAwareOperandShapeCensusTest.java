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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.ContainsAllCondition;
import com.arcadedb.query.sql.parser.ContainsAnyCondition;
import com.arcadedb.query.sql.parser.ContainsCondition;
import com.arcadedb.query.sql.parser.ContainsValueCondition;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.IsNullCondition;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10757, the census: every condition that can hand its operand to an index answers the same rows with the index present and
 * absent, for the same operand bound as a {@code List} and as an array ({@code int[]}, {@code Integer[]}, {@code long[]},
 * {@code Object[]}). The operand shapes a client binds are the dimension the fixed CONTAINSALL bug lived in: a census that binds
 * literals and lists only passes with it.
 * <p>
 * The conditions are found by scanning the parser package for the classes that override
 * {@link BooleanExpression#isIndexAware(IndexSearchInfo)}, so a new index-aware operator fails {@link #everyIndexAwareConditionIsInTheCensus()}
 * until it has statements here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10757IndexAwareOperandShapeCensusTest extends TestHelper {

  /** The statements of each index-aware condition, written with the type name as {@code %s}. */
  private static final Map<Class<?>, List<String>> STATEMENTS = new LinkedHashMap<>();

  static {
    STATEMENTS.put(ContainsAllCondition.class, List.of("SELECT name FROM %s WHERE tags CONTAINSALL :p"));
    STATEMENTS.put(ContainsAnyCondition.class, List.of("SELECT name FROM %s WHERE tags CONTAINSANY :p"));
    STATEMENTS.put(ContainsCondition.class, List.of("SELECT name FROM %s WHERE tags CONTAINS :p"));
    // `tags = :p` and `tags IN :p` over a BY ITEM index are left out: the index answers them by item and the scan by whole value,
    // and which of the two meanings both should share is a decision of its own (issue #10784): add them back with it
    STATEMENTS.put(InCondition.class, List.of("SELECT name FROM %s WHERE n IN :p"));
    STATEMENTS.put(BinaryCondition.class,
        List.of("SELECT name FROM %s WHERE n = :p", "SELECT name FROM %s WHERE n >= :p", "SELECT name FROM %s WHERE m = :p",
            "SELECT name FROM %s WHERE m CONTAINSKEY :p"));
    STATEMENTS.put(ContainsValueCondition.class, List.of("SELECT name FROM %s WHERE m CONTAINSVALUE :p"));
    STATEMENTS.put(BetweenCondition.class, List.of("SELECT name FROM %s WHERE n BETWEEN :p AND 3"));
    STATEMENTS.put(IsNullCondition.class, List.of("SELECT name FROM %s WHERE n IS NULL AND :p IS NOT NULL"));
  }

  private void createTypes() {
    for (final String type : List.of("Indexed", "Plain")) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING").close();
      database.command("sql", "CREATE PROPERTY " + type + ".n INTEGER").close();
      database.command("sql", "CREATE PROPERTY " + type + ".tags LIST OF INTEGER").close();
      database.command("sql", "CREATE PROPERTY " + type + ".m MAP OF INTEGER").close();
    }
    database.command("sql", "CREATE INDEX ON Indexed (n) NOTUNIQUE NULL_STRATEGY INDEX").close();
    database.command("sql", "CREATE INDEX ON Indexed (tags BY ITEM) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON Indexed (m BY VALUE) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON Indexed (m BY KEY) NOTUNIQUE").close();

    database.transaction(() -> {
      for (final String type : List.of("Indexed", "Plain")) {
        int i = 0;
        for (final List<Integer> tags : List.of(List.of(1), List.of(1, 2), List.of(2), List.of(2, 3, 1), List.<Integer>of(),
            List.of(3, 3))) {
          final Map<String, Object> m = new HashMap<>();
          for (int k = 0; k < tags.size(); k++)
            m.put("k" + k, tags.get(k));
          database.newDocument(type).set("name", "r" + i).set("n", i % 4).set("tags", new ArrayList<>(tags)).set("m", m).save();
          ++i;
        }
        database.newDocument(type).set("name", "missing").save();
      }
    });
  }

  private Set<String> names(final String statement, final String type, final Object operand) {
    final Set<String> names = new TreeSet<>();
    final Map<String, Object> params = new HashMap<>();
    params.put("p", operand);
    try (final ResultSet rs = database.query("sql", statement.formatted(type), params)) {
      rs.stream().forEach(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  private static Map<String, Object> shapesOf(final int... items) {
    final Map<String, Object> shapes = new LinkedHashMap<>();
    final List<Integer> list = new ArrayList<>();
    for (final int item : items)
      list.add(item);
    final long[] longs = new long[items.length];
    for (int i = 0; i < items.length; i++)
      longs[i] = items[i];
    shapes.put("List", list);
    shapes.put("int[]", items.clone());
    shapes.put("Integer[]", list.toArray(new Integer[0]));
    shapes.put("long[]", longs);
    shapes.put("Object[]", list.toArray());
    return shapes;
  }

  @Test
  void theIndexedAndTheUnindexedTypeAgreeForEveryOperandShape() {
    createTypes();
    final List<int[]> operands = List.of(new int[] {}, new int[] { 1 }, new int[] { 3 }, new int[] { 1, 2 }, new int[] { 2, 1 },
        new int[] { 1, 2, 3 }, new int[] { 3, 3 }, new int[] { 4 }, new int[] { 1, 4 });
    for (final Map.Entry<Class<?>, List<String>> condition : STATEMENTS.entrySet())
      for (final String statement : condition.getValue()) {
        // the scalar binding too, for the conditions whose operand is one value
        for (final int scalar : new int[] { 1, 3, 4 })
          assertThat(names(statement, "Indexed", scalar)).as("%s with %d", statement, scalar).isEqualTo(names(statement, "Plain", scalar));
        for (final int[] items : operands)
          for (final Map.Entry<String, Object> shape : shapesOf(items).entrySet())
            assertThat(names(statement, "Indexed", shape.getValue())).as("%s with %s %s", statement, shape.getKey(),
                Arrays.toString(items)).isEqualTo(names(statement, "Plain", shape.getValue()));
        // and a null, alone or as an item, which no index entry stands for
        for (final Object withNull : Arrays.asList(null, Arrays.asList((Object) null), Arrays.asList(null, 1), Arrays.asList(1, null),
            new Integer[] { null, 1 }))
          assertThat(names(statement, "Indexed", withNull)).as("%s with %s", statement,
              withNull instanceof Object[] a ? Arrays.toString(a) : withNull).isEqualTo(names(statement, "Plain", withNull));
      }
  }

  @Test
  void everyIndexAwareConditionIsInTheCensus() throws URISyntaxException {
    final File classes = new File(BooleanExpression.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    // run from a jar there is no directory to list: the census then holds for the classes it names
    if (!classes.isDirectory())
      return;
    final String packageName = BooleanExpression.class.getPackageName();
    final File directory = new File(classes, packageName.replace('.', File.separatorChar));
    final Set<String> overriding = new TreeSet<>();
    for (final String file : directory.list((dir, name) -> name.endsWith(".class") && !name.contains("$"))) {
      try {
        final Class<?> c = Class.forName(packageName + "." + file.substring(0, file.length() - ".class".length()));
        if (c != BooleanExpression.class && BooleanExpression.class.isAssignableFrom(c))
          c.getDeclaredMethod("isIndexAware", IndexSearchInfo.class);
        else
          continue;
        overriding.add(c.getSimpleName());
      } catch (final NoSuchMethodException | ClassNotFoundException e) {
        // not an override
      }
    }
    final Set<String> covered = new TreeSet<>();
    for (final Class<?> c : STATEMENTS.keySet())
      covered.add(c.getSimpleName());
    assertThat(overriding).isNotEmpty().isEqualTo(covered);
  }
}
