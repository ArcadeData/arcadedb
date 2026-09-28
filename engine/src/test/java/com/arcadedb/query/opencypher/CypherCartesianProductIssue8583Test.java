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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.executor.operators.CartesianProduct;
import com.arcadedb.query.opencypher.executor.operators.EquiJoinKey;
import com.arcadedb.query.opencypher.executor.operators.NodeByLabelScan;
import com.arcadedb.query.opencypher.executor.operators.RowBuffer;
import com.arcadedb.query.opencypher.executor.operators.ValueHashJoin;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8583: the buffer a {@code CartesianProduct} replays its right input from held every right row as the operator
 * produced it, a vertex with its whole record, about 1.25 KB each, so one product over a large label held gigabytes.
 * Past a few thousand rows a read-only statement's buffer now holds its records by RID, in arrays of primitives, and
 * loads them again when a row is replayed: the rows it answers must be the ones it was given.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCartesianProductIssue8583Test extends TestHelper {
  private static final int ITEMS = 20;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().createVertexType("Other");
      database.getSchema().createEdgeType("LINK");
      MutableVertex previous = null;
      for (int i = 0; i < ITEMS; i++) {
        final MutableVertex item = database.newVertex("Item").set("id", i).set("name", "item" + i).set("x", i % 3).save();
        if (previous != null)
          previous.newEdge("LINK", item).set("w", i).save();
        previous = item;
      }
      for (int i = 0; i < 3; i++)
        database.newVertex("Other").set("id", i).save();
    });
  }

  @Test
  void compactBufferGivesBackTheRowsItWasGiven() {
    final List<Result> given = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Item)-[r:LINK]->(b:Item) RETURN a, r, b.id AS id")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final ResultInternal copy = new ResultInternal();
        copy.setProperty("a", row.getProperty("a"));
        copy.setProperty("r", row.getProperty("r"));
        copy.setProperty("id", row.getProperty("id"));
        copy.setProperty("nothing", null);
        given.add(copy);
      }
    }
    assertThat(given).hasSize(ITEMS - 1);

    final RowBuffer buffer = new RowBuffer(database, OperationHeapLimit.of(context(), "test"), 5);
    for (int i = 0; i < given.size(); i++) {
      buffer.add(given.get(i));
      assertThat(buffer.isCompact()).as("compact past 5 rows, after %s", i + 1).isEqualTo(i + 1 > 5);
    }
    assertThat(buffer.size()).isEqualTo(given.size());

    for (int i = 0; i < given.size(); i++) {
      final Result expected = given.get(i);
      final Result actual = buffer.get(i);
      assertThat(actual.getPropertyNames()).containsExactly("a", "r", "id", "nothing");
      final Vertex vertex = actual.getProperty("a");
      assertThat(vertex).isInstanceOf(ImmutableVertex.class);
      assertThat(vertex.getIdentity()).isEqualTo(((Vertex) expected.getProperty("a")).getIdentity());
      assertThat(vertex.getInteger("id")).isEqualTo(((Vertex) expected.getProperty("a")).getInteger("id"));
      final Edge edge = actual.getProperty("r");
      assertThat(edge.getIdentity()).isEqualTo(((Edge) expected.getProperty("r")).getIdentity());
      assertThat(edge.getInteger("w")).isEqualTo(((Edge) expected.getProperty("r")).getInteger("w"));
      assertThat(actual.<Integer>getProperty("id")).isEqualTo(expected.<Integer>getProperty("id"));
      assertThat(actual.<Object>getProperty("nothing")).isNull();
    }
  }

  @Test
  void aRowOfAnotherShapeIsHeldAsItCame() {
    final RowBuffer buffer = new RowBuffer(database, OperationHeapLimit.of(context(), "test"), 1);
    final Vertex vertex = firstItem();
    buffer.add(row("v", vertex));
    buffer.add(row("v", vertex));
    final ResultInternal other = new ResultInternal();
    other.setProperty("w", 42);
    buffer.add(other);
    buffer.add(row("v", "not a record"));

    assertThat(buffer.isCompact()).isTrue();
    assertThat(buffer.get(2)).isSameAs(other);
    assertThat(((Vertex) buffer.get(1).getProperty("v")).getIdentity()).isEqualTo(vertex.getIdentity());
    assertThat(buffer.get(3).<String>getProperty("v")).isEqualTo("not a record");
  }

  @Test
  void aRecordDeletedSinceItWasBufferedDropsItsRow() {
    final RowBuffer buffer = new RowBuffer(database, OperationHeapLimit.of(context(), "test"), 1);
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (o:Other) RETURN o")) {
      while (rs.hasNext()) {
        final Vertex vertex = rs.next().getProperty("o");
        rids.add(vertex.getIdentity());
        buffer.add(row("o", vertex));
      }
    }
    assertThat(buffer.isCompact()).isTrue();

    database.transaction(() -> rids.get(1).asVertex().delete());

    assertThat(buffer.get(0)).isNotNull();
    assertThat(buffer.get(1)).as("the record is gone, and so is its row").isNull();
    assertThat(buffer.get(2)).isNotNull();
    assertThat(buffer.size()).isEqualTo(3);
    assertThat(buffer.liveSize()).as("a row found deleted is not replayed again").isEqualTo(2);
    assertThat(buffer.get(1)).isNull();
    assertThat(buffer.liveSize()).isEqualTo(2);
  }

  @Test
  void aProductStopsOnceEveryCompactRightRowIsDeleted() {
    final List<RID> others = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (o:Other) RETURN o")) {
      while (rs.hasNext())
        others.add(rs.next().<Vertex>getProperty("o").getIdentity());
    }
    final CartesianProduct product = new CartesianProduct(new NodeByLabelScan("a", "Item", 1, ITEMS),
        new NodeByLabelScan("b", "Other", 1, 3), 1, 3L * ITEMS);
    product.setCompactAfterRows(1);
    int rows = 0;
    try (final ResultSet rs = product.execute(context(), 100)) {
      // The first Item crosses every Other, which fills the buffer; then another transaction deletes them all
      for (int i = 0; i < 3; i++) {
        rs.next();
        ++rows;
      }
      database.transaction(() -> others.forEach(rid -> rid.asVertex().delete()));
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    assertThat(rows).isEqualTo(3);
  }

  @Test
  void aBufferThatMayNotCompactHoldsTheRowsItself() {
    final RowBuffer buffer = new RowBuffer(null, OperationHeapLimit.of(context(), "test"), 1);
    final Result first = row("v", firstItem());
    buffer.add(first);
    buffer.add(row("v", firstItem()));
    assertThat(buffer.isCompact()).isFalse();
    assertThat(buffer.get(0)).isSameAs(first);
  }

  @Test
  void compactCartesianProductProducesTheSamePairs() {
    assertThat(productPairs(3)).isEqualTo(productPairs(0)).hasSize(ITEMS * ITEMS);
  }

  @Test
  void compactHashJoinProducesTheSamePairs() {
    assertThat(hashJoinPairs(3)).isEqualTo(hashJoinPairs(0));
    // 20 items with x = i % 3: 7 + 7 + 6 of each value, paired within their value
    assertThat(hashJoinPairs(3)).hasSize(7 * 7 + 7 * 7 + 6 * 6);
  }

  @Test
  void onlyAStatementThatCannotChangeARecordCompactsItsBuffer() {
    assertThat(plan("MATCH (a:Item), (b:Other) RETURN a, b")).contains("CartesianProduct [compact buffer]");
    // Adding entities changes no property of a buffered record
    assertThat(plan("MATCH (a:Item), (b:Other) CREATE (a)-[:SEEN]->(b)")).contains("CartesianProduct [compact buffer]");
    assertThat(plan("MATCH (a:Item), (b:Other) MERGE (a)-[:SEEN]->(b)")).contains("CartesianProduct [compact buffer]");
    // A write between the buffering and the replay could change what a reloaded record reads
    for (final String write : List.of("MATCH (a:Item), (b:Other) SET b.seen = true", "MATCH (a:Item), (b:Other) REMOVE b.id",
        "MATCH (a:Item), (b:Other) MERGE (a)-[r:SEEN]->(b) ON MATCH SET b.seen = true"))
      assertThat(plan(write)).as(write).contains("CartesianProduct").doesNotContain("compact buffer");
  }

  @Test
  void aCreateOverACompactProductSeesEveryRecord() {
    database.transaction(() -> {
      final CartesianProduct product = new CartesianProduct(new NodeByLabelScan("a", "Other", 1, 3),
          new NodeByLabelScan("b", "Item", 1, ITEMS), 1, 3L * ITEMS);
      product.setCompactAfterRows(2);
      try (final ResultSet rs = product.execute(context(), 100)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          // Creating the edge rewrites the Item record the buffer reloads for the next Other
          row.<Vertex>getProperty("a").modify().newEdge("LINK", row.<Vertex>getProperty("b")).save();
        }
      }
    });
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Other)-[:LINK]->(b:Item) RETURN count(*) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(3L * ITEMS);
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<String> productPairs(final int compactAfterRows) {
    final CartesianProduct product = new CartesianProduct(new NodeByLabelScan("a", "Item", 1, ITEMS),
        new NodeByLabelScan("b", "Item", 1, ITEMS), 1, ITEMS * ITEMS);
    product.setCompactAfterRows(compactAfterRows);
    return pairs(product.execute(context(), 100));
  }

  private List<String> hashJoinPairs(final int compactAfterRows) {
    final ValueHashJoin join = new ValueHashJoin(new NodeByLabelScan("a", "Item", 1, ITEMS),
        new NodeByLabelScan("b", "Item", 1, ITEMS),
        new EquiJoinKey[] { EquiJoinKey.of(new PropertyAccessExpression("a", "x"), new PropertyAccessExpression("b", "x")) },
        1, ITEMS, null);
    join.setCompactAfterRows(compactAfterRows);
    return pairs(join.execute(context(), 100));
  }

  private static List<String> pairs(final ResultSet rs) {
    final List<String> pairs = new ArrayList<>();
    try (rs) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        pairs.add(((Vertex) row.getProperty("a")).getInteger("id") + "/" + ((Vertex) row.getProperty("b")).getInteger("id"));
      }
    }
    return pairs;
  }

  private Vertex firstItem() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Item {id: 0}) RETURN a")) {
      return rs.next().getProperty("a");
    }
  }

  private static ResultInternal row(final String name, final Object value) {
    final ResultInternal row = new ResultInternal();
    row.setProperty(name, value);
    return row;
  }

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }
}
