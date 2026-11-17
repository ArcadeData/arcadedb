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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.method.collection.SQLMethodSize;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12466: a SELECT whose projection or WHERE calls a SQL method that is not a graph method ({@code size()},
 * {@code toUpperCase()}, ...) was never stored in the execution plan cache, so every execution planned it again: twice the
 * time and 2.3 times the allocation of the same key lookup without the method call. {@code MethodCall.isCacheable()}
 * answered {@code isGraphFunction()} alone, where a function has been cacheable since #6190 when it declares itself
 * deterministic.
 */
class MethodCallPlanCacheIssue12466Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("Chunk");
      type.createProperty("name", Type.STRING);
      type.createProperty("tags", Type.LIST);
      type.createProperty("meta", Type.MAP);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Chunk", "name");
      database.newDocument("Chunk").set("name", "alpha").set("tags", List.of("a", "b", "c")).set("meta", Map.of("x", 1, "y", 2)).save();
      database.newDocument("Chunk").set("name", "BETA").set("tags", List.of("a")).set("meta", Map.of("z", 3)).save();
      database.newDocument("Chunk").set("name", "gamma").set("tags", List.of()).set("meta", Map.of()).save();
    });
  }

  private boolean cachedAfterRunning(final String sql, final Object... args) {
    ((DatabaseInternal) database).getExecutionPlanCache().invalidate();
    // twice: the first execution plans the statement and may put it, the second is the one a cache would answer
    for (int i = 0; i < 2; i++)
      try (final ResultSet rs = database.query("sql", sql, args)) {
        while (rs.hasNext())
          rs.next();
      }
    return ((DatabaseInternal) database).getExecutionPlanCache().contains(sql);
  }

  @Test
  void aProjectionCallingAPureMethodIsPlanCached() {
    assertThat(cachedAfterRunning("SELECT name, tags.size() AS n FROM Chunk WHERE name = ?", "alpha"))
        .as("tags.size() in the projection").isTrue();
    assertThat(cachedAfterRunning("SELECT name, name.toUpperCase() AS u FROM Chunk WHERE name = ?", "alpha"))
        .as("name.toUpperCase() in the projection").isTrue();
    assertThat(cachedAfterRunning("SELECT name FROM Chunk WHERE name = ?.toLowerCase()", "ALPHA"))
        .as("a parameter given to a pure method in the WHERE").isTrue();
  }

  @Test
  void aMethodThatDoesNotDeclareItselfPureStaysUncached() {
    // format() reads the database's date and time settings: not a function of its arguments alone
    assertThat(cachedAfterRunning("SELECT name, name.format('%s') AS f FROM Chunk WHERE name = ?", "alpha")).isFalse();
  }

  @Test
  void aCachedPlanAnswersEachBindingOfAMethodOverAParameter() {
    final String sql = "SELECT name FROM Chunk WHERE name = ?.toLowerCase()";
    assertThat(cachedAfterRunning(sql, "ALPHA")).isTrue();

    final List<String> seen = new ArrayList<>();
    for (final String binding : new String[] { "ALPHA", "GAMMA", "nope", "ALPHA" })
      try (final ResultSet rs = database.query("sql", sql, binding)) {
        final List<String> names = new ArrayList<>();
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
        seen.add(String.join(",", names));
      }

    assertThat(seen).containsExactly("alpha", "gamma", "", "alpha");
  }

  @Test
  void theProjectedValueFollowsTheRowOfEachExecution() {
    final String sql = "SELECT name.toUpperCase() AS u, tags.size() AS n FROM Chunk WHERE name = ?";
    assertThat(cachedAfterRunning(sql, "alpha")).isTrue();

    final List<String> seen = new ArrayList<>();
    for (final String name : new String[] { "alpha", "BETA", "gamma", "alpha" })
      try (final ResultSet rs = database.query("sql", sql, name)) {
        final Result row = rs.next();
        seen.add(row.getProperty("u") + ":" + row.getProperty("n"));
      }

    assertThat(seen).containsExactly("ALPHA:3", "BETA:1", "GAMMA:0", "ALPHA:3");
  }

  /**
   * The factory lets an application replace a built-in. A replacement that extends {@link AbstractSQLMethod} under a listed name
   * inherits {@code isDeterministic() == true} from the name list, which says nothing about what it does: only the implementation
   * the factory registered itself is trusted.
   */
  @Test
  void aReplacedBuiltInIsNotCachedWhateverItInherits() {
    final String sql = "SELECT name, tags.size() AS n FROM Chunk WHERE name = ?";
    assertThat(cachedAfterRunning(sql, "alpha")).as("the built-in size()").isTrue();

    final DefaultSQLMethodFactory factory = DefaultSQLMethodFactory.getInstance();
    final Object original = factory.getMethods().get(SQLMethodSize.NAME);
    final AbstractSQLMethod replacement = new AbstractSQLMethod(SQLMethodSize.NAME) {
      @Override
      public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
        return -1;
      }
    };
    assertThat(replacement.isDeterministic()).as("inherited from the name list").isTrue();
    factory.register(SQLMethodSize.NAME, replacement);
    try {
      assertThat(cachedAfterRunning(sql, "alpha")).as("the replacement").isFalse();
    } finally {
      factory.register(SQLMethodSize.NAME, original);
    }
    assertThat(cachedAfterRunning(sql, "alpha")).as("the built-in again").isTrue();
  }

  @Test
  void aMethodNothingAnswersIsNotCachedAndAsksNothingOfThePlanner() {
    assertThat(((DatabaseInternal) database).getStatementCache().get("SELECT name.nosuchmethod() AS x FROM Chunk")
        .executionPlanCanBeCached()).isFalse();
  }

  /** The census: adding a method to the plan cache is a visible change to this list. */
  @Test
  void theDeterministicBuiltInsAreExactlyTheReviewedList() {
    final DefaultSQLMethodFactory factory = DefaultSQLMethodFactory.getInstance();
    final Set<String> deterministic = new TreeSet<>();
    for (final String name : factory.getBuiltInMethodNames())
      if (factory.createMethod(name).isDeterministic())
        deterministic.add(name);

    assertThat(deterministic).containsExactly("append", "capitalize", "charat", "first", "indexof", "keys", "last", "lastindexof", "left",
        "length", "prefix", "right", "size", "substring", "tolowercase", "touppercase", "trim", "values");
  }

  /** Every method the PR lists answers what an uncached plan answers, for each row and a bound parameter. */
  @Test
  void aCachedPlanAnswersWhatAFreshPlanAnswers() {
    for (final String sql : new String[] {
        "SELECT name, tags.first() AS f, tags.last() AS l, tags.size() AS s FROM Chunk WHERE name = ?",
        "SELECT name, meta.keys() AS k, meta.values() AS v FROM Chunk WHERE name = ?",
        "SELECT name.left(2) AS l, name.right(2) AS r, name.subString(1, 3) AS s, name.charAt(0) AS c, name.indexOf('a') AS i,"
            + " name.lastIndexOf('a') AS j, name.length() AS n, name.trim() AS t, name.prefix('x') AS p, name.append('y') AS a,"
            + " name.capitalize() AS cap, name.toLowerCase() AS lo FROM Chunk WHERE name = ?" })
      for (final String name : new String[] { "alpha", "BETA", "gamma" }) {
        ((DatabaseInternal) database).getExecutionPlanCache().invalidate();
        final String fresh = answer(sql, name);
        final String again = answer(sql, name);
        final String cached = answer(sql, name);
        assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).as(sql).isTrue();
        assertThat(again).as(sql + " [" + name + "]").isEqualTo(fresh);
        assertThat(cached).as(sql + " [" + name + "]").isEqualTo(fresh);
      }
  }

  private String answer(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.next().toMap().toString();
    }
  }
}
