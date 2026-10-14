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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9988: {@code include(null, 'a')} returned {@code null}, discarding the whole projection, while
 * {@code include('a', null)} skipped the null and {@code exclude(null, 'a')} had no gate at all. The third defect of
 * that shape in the variadic SQL methods (#7028 append, #7889 remove/removeAll), each fixed where it was pointed.
 * <p>
 * The variadic methods do not share one meaning for a null argument, and must not: {@code remove(null)} removes the
 * null elements of a collection, {@code append(null)} and {@code include(null)} skip it, {@code transform(null)}
 * refuses it. What they do share - and what all three defects broke - is that the meaning does not depend on WHERE
 * the null is. So the guarantee is asserted over every built-in method with an unbounded parameter list, enumerated
 * from the factory: a new variadic method fails {@link #everyVariadicBuiltInMethodIsCovered()} until it gets a fixture
 * here, and then has to answer the same whichever position the null takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9988VariadicMethodNullArgumentTest extends TestHelper {

  /** One receiver and two non-null arguments per variadic method; the null is inserted at every position. */
  private record Fixture(Object receiver, Object a, Object b) {
  }

  private static Map<String, Fixture> fixtures() {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("a", 1);
    map.put("b", 2);
    map.put("c", 3);

    final Map<String, Fixture> fixtures = new LinkedHashMap<>();
    fixtures.put("include", new Fixture(map, "a", "b"));
    fixtures.put("exclude", new Fixture(map, "a", "b"));
    fixtures.put("remove", new Fixture(Arrays.asList(1, null, 2, 3), 2, 3));
    fixtures.put("removeall", new Fixture(Arrays.asList(1, null, 2, 2, 3), 2, 3));
    fixtures.put("append", new Fixture("x", "A", "B"));
    fixtures.put("transform", new Fixture(List.of(" a "), "trim", "toUpperCase"));
    return fixtures;
  }

  @Test
  void everyVariadicBuiltInMethodIsCovered() {
    final TreeSet<String> variadic = new TreeSet<>();
    final DefaultSQLMethodFactory factory = DefaultSQLMethodFactory.getInstance();
    for (final String name : factory.getBuiltInMethodNames())
      if (factory.createMethod(name).getMaxParams() == -1)
        variadic.add(name);

    assertThat(variadic).as("a new variadic method needs a fixture in this test, so its null handling is decided")
        .isEqualTo(new TreeSet<>(fixtures().keySet()));
  }

  @Test
  void aNullArgumentMeansTheSameWhereverItAppears() {
    final CommandContext context = new BasicCommandContext().setDatabase(database);

    for (final Map.Entry<String, Fixture> entry : fixtures().entrySet()) {
      final SQLMethod method = DefaultSQLMethodFactory.getInstance().createMethod(entry.getKey());
      final Fixture f = entry.getValue();

      final List<Object> outcomes = new ArrayList<>();
      outcomes.add(outcome(method, f, context, null, f.a(), f.b()));
      outcomes.add(outcome(method, f, context, f.a(), null, f.b()));
      outcomes.add(outcome(method, f, context, f.a(), f.b(), null));

      assertThat(outcomes).as("%s(): leading, middle and trailing null", entry.getKey())
          .allSatisfy(o -> assertThat(o).isEqualTo(outcomes.getFirst()));
      assertThat(outcomes.getFirst()).as("%s(): a leading null must not discard the whole call", entry.getKey())
          .isNotNull();
    }
  }

  /** The value the method answered, or the class of what it threw: two calls agree when these are equal. */
  private static Object outcome(final SQLMethod method, final Fixture fixture, final CommandContext context,
      final Object... params) {
    try {
      return method.execute(fixture.receiver(), null, context, params);
    } catch (final RuntimeException e) {
      return e.getClass();
    }
  }

  @Test
  void includeWithALeadingNullKeepsTheProjection() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> database.newDocument("D").set("a", 1, "b", 2, "c", 3).save());

    assertThat(projection("select @this.include(null, 'a') as p from D")).containsOnlyKeys("a").containsEntry("a", 1);
    assertThat(projection("select @this.include('a', null) as p from D")).containsOnlyKeys("a").containsEntry("a", 1);
    assertThat(projection("select @this.exclude(null, 'a') as p from D")).containsOnlyKeys("b", "c");
    assertThat(projection("select @this.include(null) as p from D")).as("nothing named, nothing included").isEmpty();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> projection(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Object p = rs.next().getProperty("p");
      assertThat(p).as(sql).isInstanceOf(Map.class);
      return (Map<String, Object>) p;
    }
  }
}
