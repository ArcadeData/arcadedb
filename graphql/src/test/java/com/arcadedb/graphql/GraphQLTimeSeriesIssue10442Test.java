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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10442 (follow-up of #10440): a GraphQL query over a type mapped to a TIMESERIES type must return the samples, not
 * silently answer nothing as the Cypher pattern used to. A GraphQL field reads its type with a SQL {@code SELECT}, which serves
 * the samples of a time-series type, so this pins that behavior.
 */
class GraphQLTimeSeriesIssue10442Test {
  private static final String DB_PATH = "./target/testgraphql-timeseries-10442";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sqlscript", """
        CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE);
        INSERT INTO Ev SET ts = 1792396800000, k = 'a', v = 1.0;
        INSERT INTO Ev SET ts = 1792396860000, k = 'b', v = 2.0;
        """);
    database.command("graphql", """
        type Query {
          allEvents: [Ev]
          eventsByK(k: String): [Ev]
        }

        type Ev {
          ts: Long
          k: String
          v: Float
        }""");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void queryOverTimeSeriesTypeReturnsTheSamples() {
    final List<Result> rows = drain(database.query("graphql", "{ allEvents { k v } }"));
    assertThat(rows).hasSize(2);
    final List<Object> ks = new ArrayList<>();
    for (final Result row : rows)
      ks.add(row.getProperty("k"));
    assertThat(ks).containsExactlyInAnyOrder("a", "b");
  }

  @Test
  void filteredQueryOverTimeSeriesTypeReturnsTheMatchingSample() {
    final List<Result> rows = drain(database.query("graphql", "{ eventsByK(k: \"b\") { k v } }"));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("k")).isEqualTo("b");
    assertThat(rows.getFirst().<Number>getProperty("v").doubleValue()).isEqualTo(2.0);
  }

  @Test
  void relationshipNamingTimeSeriesTypeIsNotSilentlyEmpty() {
    database.getSchema().createVertexType("Station");
    database.transaction(() -> database.newVertex("Station").set("name", "s1").save());
    database.command("graphql", """
        type Query {
          stations: [Station]
        }

        type Station {
          name: String
          events: [Ev] @relationship(type: "Ev", direction: OUT)
        }""");

    // a vertex has no edge of a time-series type: the field cannot answer the samples, so it must not answer an empty list
    assertThatThrownBy(() -> drain(database.query("graphql", "{ stations { name events { k } } }"))).hasStackTraceContaining(
        "'Ev', a TIMESERIES type");
  }

  private static List<Result> drain(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    try (rs) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
