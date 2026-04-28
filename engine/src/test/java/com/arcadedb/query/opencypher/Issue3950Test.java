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

/**
 * Regression tests for issue #3950: a standalone {@code ORDER BY ... LIMIT ...} clause between
 * two {@code MATCH} clauses must apply the sort and limit in place, not be deferred to the end
 * of the statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3950Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-3950").create();
    database.getSchema().createVertexType("Order3950");
    database.getSchema().createVertexType("Item3950");
    database.getSchema().createEdgeType("CONTAINS3950");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Order3950 {id: 'ORD-001', orderDate: 20230101})");
      database.command("opencypher", "CREATE (:Order3950 {id: 'ORD-002', orderDate: 20230102})");
      database.command("opencypher", "CREATE (:Order3950 {id: 'ORD-003', orderDate: 20230103})");
      database.command("opencypher", "CREATE (:Item3950 {name: 'ItemA'})");
      database.command("opencypher", "CREATE (:Item3950 {name: 'ItemB'})");
      database.command("opencypher", "CREATE (:Item3950 {name: 'ItemC'})");
      database.command("opencypher",
          "MATCH (o:Order3950 {id:'ORD-001'}), (i:Item3950 {name:'ItemA'}) CREATE (o)-[:CONTAINS3950]->(i)");
      database.command("opencypher",
          "MATCH (o:Order3950 {id:'ORD-002'}), (i:Item3950 {name:'ItemB'}) CREATE (o)-[:CONTAINS3950]->(i)");
      database.command("opencypher",
          "MATCH (o:Order3950 {id:'ORD-003'}), (i:Item3950 {name:'ItemC'}) CREATE (o)-[:CONTAINS3950]->(i)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void standaloneOrderByDescLimitBeforeFollowingMatch() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (o:Order3950) \
        ORDER BY o.orderDate DESC \
        LIMIT 1 \
        MATCH (o)-[:CONTAINS3950]->(i:Item3950) \
        RETURN o.id AS orderId, collect(i.name) AS items""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("orderId")).isEqualTo("ORD-003");
    assertThat((List<Object>) rows.get(0).getProperty("items")).containsExactly("ItemC");
  }

  @Test
  void standaloneOrderByAscLimitBeforeFollowingMatch() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (o:Order3950) \
        ORDER BY o.orderDate ASC \
        LIMIT 1 \
        MATCH (o)-[:CONTAINS3950]->(i:Item3950) \
        RETURN o.id AS orderId, collect(i.name) AS items""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("orderId")).isEqualTo("ORD-001");
    assertThat((List<Object>) rows.get(0).getProperty("items")).containsExactly("ItemA");
  }

  @Test
  void standaloneLimitOnlyBeforeFollowingMatch() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (o:Order3950) \
        ORDER BY o.orderDate ASC \
        SKIP 1 \
        LIMIT 1 \
        MATCH (o)-[:CONTAINS3950]->(i:Item3950) \
        RETURN o.id AS orderId, collect(i.name) AS items""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("orderId")).isEqualTo("ORD-002");
  }

  @Test
  void standaloneOrderByLimitBeforeReturnStillWorks() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (o:Order3950) \
        ORDER BY o.orderDate DESC \
        LIMIT 1 \
        RETURN o.id AS orderId""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("orderId")).isEqualTo("ORD-003");
  }

  @Test
  void explicitWithOrderByLimitStillWorks() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (o:Order3950) \
        WITH o \
        ORDER BY o.orderDate DESC \
        LIMIT 1 \
        MATCH (o)-[:CONTAINS3950]->(i:Item3950) \
        RETURN o.id AS orderId, collect(i.name) AS items""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("orderId")).isEqualTo("ORD-003");
    assertThat((List<Object>) rows.get(0).getProperty("items")).containsExactly("ItemC");
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
