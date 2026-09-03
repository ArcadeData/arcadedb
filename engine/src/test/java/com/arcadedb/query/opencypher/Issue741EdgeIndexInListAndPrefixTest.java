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
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Follow-up to #740/#741 (Locstat): the OpenCypher edge-index seed introduced for
 * {@code MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = ... AND t.date = ...} only fired when equality
 * predicates covered the <em>whole</em> index key. The same client's next two queries - the leading column
 * alone, and an {@code IN} list on it - still scanned every edge, while their SQL twins used the index:
 * <pre>
 *   MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] RETURN t
 *   MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = 'S28719720' OR t.transactionId = '74529884' RETURN t
 * </pre>
 * The seed now accepts a leading prefix of the key (walked as a range of the ordered index, as the vertex
 * {@code NodeIndexSeek} already did) and an {@code IN} list or an {@code OR} of equalities on a column (one seek
 * per value, results de-duplicated). The MATCH's WHERE filter is still re-applied above the seed, so every
 * shape here is checked for the exact rows it returns, not only for the plan it takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue741EdgeIndexInListAndPrefixTest extends TestHelper {
  private static final int NOISE = 200;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account").createProperty("accountNumber", Type.STRING);
      final EdgeType transfer = database.getSchema().createEdgeType("TRANSFER");
      transfer.createProperty("transactionId", Type.STRING);
      transfer.createProperty("date", Type.DATE);
      database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIndexName("TRANSFER_PK").create();
    });

    database.transaction(() -> {
      final var a1 = database.newVertex("Account").set("accountNumber", "A1").save();
      final var a2 = database.newVertex("Account").set("accountNumber", "A2").save();
      a1.newEdge("TRANSFER", a2).set("transactionId", "S28719720", "date", "2026-01-01").save();
      a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-28").save();
      a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-27").save();
      for (int i = 0; i < NOISE; i++) {
        final var c = database.newVertex("Account").set("accountNumber", "N" + i).save();
        a1.newEdge("TRANSFER", c).set("transactionId", "NOISE-" + i, "date", "2026-03-%02d".formatted(i % 28 + 1)).save();
      }
    });
  }

  @Test
  void inListOnLeadingColumnUsesTheIndexAndReturnsExactlyTheListedEdges() {
    assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] RETURN t",
        Map.of(), "74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");
  }

  @Test
  void orOfEqualitiesOnTheSameColumnSeeksLikeAnInList() {
    assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = 'S28719720' OR t.transactionId = '74529884' RETURN t",
        Map.of(), "74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");
  }

  @Test
  void equalityOnTheLeadingColumnAloneWalksThePrefixRange() {
    assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = '74529884' RETURN t",
        Map.of(), "74529884/2026-02-27", "74529884/2026-02-28");
  }

  @Test
  void inListOnTheLeadingColumnAndEqualityOnTheTrailingOneSeekTheWholeKey() {
    assertSeeksTheIndex(
        "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] AND t.date = date('2026-02-28') RETURN t",
        Map.of(), "74529884/2026-02-28");
  }

  @Test
  void parameterListSeeksOneKeyPerElement() {
    assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN $ids RETURN t",
        Map.of("ids", List.of("74529884", "S28719720", "MISSING")), "74529884/2026-02-27", "74529884/2026-02-28",
        "S28719720/2026-01-01");
  }

  @Test
  void repeatedListValuesReturnEachEdgeOnce() {
    assertSeeksTheIndex("MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['74529884', '74529884', 'S28719720', 'S28719720'] RETURN t",
        Map.of(), "74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");
  }

  @Test
  void incomingDirectionBindsTheEndpointsTheWrittenWay() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "PROFILE MATCH (a)<-[t:TRANSFER]-(b) WHERE t.transactionId IN ['S28719720', '74529884'] RETURN a.accountNumber AS a, b.accountNumber AS b");
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final var r = rs.next();
        rows.add(r.getProperty("a") + "<-" + r.getProperty("b"));
      }
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("MATCH EDGE BY INDEX");
      assertThat(rows).containsExactlyInAnyOrder("A2<-A1", "A2<-A1", "A2<-A1");
    });
  }

  @Test
  void orAcrossDifferentPropertiesCannotSeedTheSeekAndStillAnswersCorrectly() {
    // The OR pins transactionId on one side only: seeking it would lose every row the other side admits.
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.transactionId = 'S28719720' OR t.date = date('2026-02-28') RETURN t");
      final List<String> rows = collect(rs);
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("MATCH EDGE BY INDEX");
      assertThat(rows).containsExactlyInAnyOrder("74529884/2026-02-28", "S28719720/2026-01-01");
    });
  }

  @Test
  void inListOnTheTrailingColumnAloneIsNotAPrefixAndFallsBack() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.date IN [date('2026-02-28'), date('2026-01-01')] RETURN t");
      final List<String> rows = collect(rs);
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("MATCH EDGE BY INDEX");
      assertThat(rows).containsExactlyInAnyOrder("74529884/2026-02-28", "S28719720/2026-01-01");
    });
  }

  @Test
  void emptyListAndUnboundParameterYieldNoRows() {
    database.transaction(() -> {
      assertThat(collect(database.query("opencypher", "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN [] RETURN t"))).isEmpty();
      assertThat(collect(database.query("opencypher", "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN $ids RETURN t",
          Map.of("ids", List.of())))).isEmpty();
    });
  }

  @Test
  void seekReadsOnlyTheListedKeysNotEveryEdge() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.transactionId IN ['S28719720', '74529884'] RETURN t");
      collect(rs);
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      // The profiled seed reports the rows it pulled from the index: the three listed edges, not the NOISE ones.
      assertThat(plan).contains("MATCH EDGE BY INDEX").contains("TRANSFER_PK on transactionId,date");
      assertThat(plan).contains("transactionId IN ['S28719720', '74529884']");
      assertThat(plan).doesNotContain("MATCH RELATIONSHIP");
    });
  }

  private void assertSeeksTheIndex(final String query, final Map<String, Object> params, final String... expectedRows) {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "PROFILE " + query, params);
      final List<String> rows = collect(rs);
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("MATCH EDGE BY INDEX").contains("TRANSFER_PK");
      assertThat(rows).containsExactlyInAnyOrder(expectedRows);
    });
  }

  private static List<String> collect(final ResultSet rs) {
    final List<String> rows = new ArrayList<>();
    while (rs.hasNext()) {
      final Edge edge = rs.next().getProperty("t");
      rows.add(edge.get("transactionId") + "/" + edge.get("date"));
    }
    rs.close();
    return rows;
  }
}
