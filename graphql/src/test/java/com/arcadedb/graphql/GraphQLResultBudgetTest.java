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
package com.arcadedb.graphql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The answer to one GraphQL query is built in memory, and the objects of a row are the product of the aliases and fragment
 * spreads written at each level, not just the size of the data: two aliases per level over a fragment chain double the row at
 * every link. The objects built are counted against {@link GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} for each
 * row and against {@link GlobalConfiguration#QUERY_MAX_HEAP_RAM} for all the rows until the result set is closed, the work is
 * stopped by the command timeout and by the termination of the query, and the validation of the fragments visits each selection
 * list once.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphQLResultBudgetTest extends AbstractGraphQLTest {
  private static final int HANG_DETECTOR_SEC = 20;

  /** Two persons that know each other through one edge: following it from either end leads to the other. */
  private static void declarePeople(final Database database, final boolean withData) {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.command("graphql", """
        type Query {
          people: [Person]
        }

        type Person {
          name: String
          friends: [Person]
        }""");
    if (withData) {
      final MutableVertex first = database.newVertex("Person").set("name", "first").save();
      final MutableVertex second = database.newVertex("Person").set("name", "second").save();
      first.newEdge("KNOWS", second);
    }
  }

  /**
   * {@code levels + 1} fragments in a chain, every one selecting the next under two aliases: a few kilobytes of document whose
   * answer holds {@code 2^(levels + 1) - 1} objects for each person.
   */
  private static String fanOutQuery(final int levels) {
    final StringBuilder query = new StringBuilder("{ people { ...L0 } }");
    for (int i = 0; i < levels; i++)
      query.append(" fragment L").append(i).append(" on Person { name a: friends @relationship(type: \"KNOWS\") { ...L")
          .append(i + 1).append(" } b: friends @relationship(type: \"KNOWS\") { ...L").append(i + 1).append(" } }");
    query.append(" fragment L").append(levels).append(" on Person { name }");
    return query.toString();
  }

  private static List<Result> drain(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  @Test
  void aRowThatFansOutPastTheElementCapIsRefused() {
    executeTest(database -> {
      declarePeople(database, true);
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 5_000L);

      // 2^16 - 1 OBJECTS FOR EACH OF THE TWO PERSONS
      assertThatThrownBy(() -> drain(database.query("graphql", fanOutQuery(15)))).isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
      return null;
    });
  }

  @Test
  void aRowUnderTheElementCapIsAnsweredInFull() {
    executeTest(database -> {
      declarePeople(database, true);
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 5_000L);

      final List<Result> rows = drain(database.query("graphql", fanOutQuery(4)));

      assertThat(rows).hasSize(2);
      for (final Result row : rows) {
        Result current = row;
        for (int level = 0; level < 4; level++) {
          assertThat(current.<String>getProperty("name")).isNotNull();
          for (final String alias : List.of("a", "b")) {
            final List<Result> friends = current.getProperty(alias);
            assertThat(friends).hasSize(1);
          }
          current = current.<List<Result>>getProperty("a").getFirst();
        }
        assertThat(current.<String>getProperty("name")).isNotNull();
        assertThat(current.getPropertyNames()).doesNotContain("a", "b");
      }
      return null;
    });
  }

  /** The cap counts the objects of one row: the rows of a long answer are not added up against it. */
  @Test
  void theElementCapIsCountedForEachRow() {
    executeTest(database -> {
      declarePeople(database, false);
      for (int i = 0; i < 300; i++)
        database.newVertex("Person").set("name", "person-" + i).save();
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 50L);

      assertThat(drain(database.query("graphql", "{ people { name } }"))).hasSize(300);
      return null;
    });
  }

  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aChainOfFragmentsOverOneEdgeIsRefusedWithoutBuildingTheRow() {
    executeTest(database -> {
      declarePeople(database, true);
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 5_000L);

      final String query = fanOutQuery(40);
      assertThat(query.length()).isLessThan(10_000);
      assertThatThrownBy(() -> drain(database.query("graphql", query))).isInstanceOf(HeapLimitExceededException.class);
      return null;
    });
  }

  /** The objects of an answer built from the schema alone, with no selection set to bound them, are counted as well. */
  @Test
  void anAnswerExpandedFromTheSchemaIsCountedToo() {
    executeTest(database -> {
      database.getSchema().createVertexType("Root");
      database.getSchema().createVertexType("Mid");
      database.getSchema().createVertexType("Leaf");
      database.getSchema().createEdgeType("HAS");
      database.command("graphql", """
          type Query {
            roots: [Root]
          }

          type Root {
            kids: [Mid] @relationship(type: "HAS", direction: OUT)
          }

          type Mid {
            kids: [Leaf] @relationship(type: "HAS", direction: OUT)
          }

          type Leaf {
            name: String
          }""");
      final MutableVertex root = database.newVertex("Root").save();
      for (int i = 0; i < 30; i++) {
        final MutableVertex mid = database.newVertex("Mid").save();
        root.newEdge("HAS", mid);
        for (int j = 0; j < 30; j++)
          mid.newEdge("HAS", database.newVertex("Leaf").set("name", "leaf-" + i + "-" + j).save());
      }

      assertThat(drain(database.query("graphql", "{ roots }"))).hasSize(1);

      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 500L);
      assertThatThrownBy(() -> drain(database.query("graphql", "{ roots }"))).isInstanceOf(HeapLimitExceededException.class);
      return null;
    });
  }

  /** The bytes of every row are charged to the budget the queries of the JVM share, and given back when the result set goes. */
  @Test
  void theObjectsOfAllTheRowsAreChargedToTheSharedBudget() {
    executeTest(database -> {
      declarePeople(database, false);
      for (int i = 0; i < 10_000; i++)
        database.newVertex("Person").set("name", "person-" + i).save();
      final long reservedBefore = QueryHeapBudget.getReservedBytes();

      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
      try {
        assertThatThrownBy(() -> drain(database.query("graphql", "{ people { name } }"))).isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      } finally {
        GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
      }
      assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(reservedBefore);

      // WITHOUT THE LIMIT THE SAME ANSWER IS COMPLETE, AND GIVES BACK WHAT IT TOOK WHEN IT IS CLOSED
      assertThat(drain(database.query("graphql", "{ people { name } }"))).hasSize(10_000);
      assertThat(QueryHeapBudget.getReservedBytes()).isEqualTo(reservedBefore);
      return null;
    });
  }

  @Test
  void aTerminatedQueryStopsBeforeItBuildsAnyRow() {
    executeTest(database -> {
      declarePeople(database, true);

      final RunningQueryRegistry registry = new RunningQueryRegistry();
      try (final RunningQuery running = registry.register(database.getName(), "root", "test", null, null, RunningQuery.Client.NONE)) {
        running.terminate("test");
        assertThatThrownBy(() -> database.query("graphql", "{ people { name } }")).isInstanceOf(QueryTerminatedException.class);
      }
      return null;
    });
  }

  /** The row is already read from the database when the query is terminated: it is the building of the answer that stops. */
  @Test
  void aQueryTerminatedWhileItIsReadStopsBeforeTheNextRowIsBuilt() {
    executeTest(database -> {
      declarePeople(database, true);

      final RunningQueryRegistry registry = new RunningQueryRegistry();
      try (final RunningQuery running = registry.register(database.getName(), "root", "test", null, null, RunningQuery.Client.NONE);
          final ResultSet resultSet = database.query("graphql", fanOutQuery(10))) {
        assertThat(resultSet.hasNext()).isTrue();

        running.terminate("test");
        assertThatThrownBy(resultSet::next).isInstanceOf(QueryTerminatedException.class);
      }
      return null;
    });
  }

  /** The selections of a fragment chain are checked once for each selection list, not once for each path that reaches them. */
  @Test
  @Timeout(value = HANG_DETECTOR_SEC, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aChainOfFragmentsIsCheckedOncePerLevel() {
    executeTest(database -> {
      declarePeople(database, false);

      final String query = fanOutQuery(40);
      assertThat(query.length()).isLessThan(10_000);
      try (final ResultSet resultSet = database.query("graphql", query)) {
        assertThat(resultSet.hasNext()).isFalse();
      }
      return null;
    });
  }
}
