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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8733: an empty {@code FOREACH} between the clauses of a query changed what an {@code OPTIONAL MATCH} bound (the
 * {@code n4}, {@code n3} and {@code r0} of the first ordered row came back {@code null}). The query is written in full
 * so the barrier is the only difference between the two shapes, and each shape runs on its own database because the
 * {@code MERGE} writes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherEmptyForeachBindingsIssue8733Test extends TestHelper {
  private static final String[] SETUP = {
      "CREATE (n0:l5:l7:l6:l9:l3 {id : 32});",
      "CREATE (n1:l1:l6:l8:l10:l2:l11 {id : 33, k9 : false, k3 : 1386386315});",
      "CREATE (n2:l6:l9:l4 {id : 35, k9 : false, k3 : -498588954});",
      "CREATE (n3:l1:l0:l3:l11:l2:l5:l6:l10:l7:l8 {id : 37});",
      "CREATE (n4:l9:l2:l3:l6:l11:l4 {id : 43});",
      "CREATE (n5:l3:l4:l6:l10:l5:l7:l9:l2:l1:l0:l8:l11 {id : 45});",
      "CREATE (n6:l11:l8:l7:l1:l2:l3:l4:l10:l5:l9:l0:l6 {id : 46});",
      "CREATE (n7:l7:l8:l1:l2:l10:l9:l4:l3 {id : 48, k9 : false});",
      "CREATE (n8:l7:l1:l2:l6:l10:l0:l5:l8:l4:l11:l3 {id : 50, k9 : false, k3 : -1769307175});",
      "CREATE (n9:l1:l6:l10:l7:l3 {id : 51});",
      "CREATE (n10:l11:l2:l1:l3:l4:l7:l10:l9:l8:l5:l6 {id : 53});",
      "CREATE (n11:l11 {id : 55});",
      "CREATE (n12:l5 {id : 58, k9 : false, k3 : -1279228999, k7 : ['h']});",
      "CREATE (n13:l4:l5:l8:l9:l11:l0:l7 {id : 59});",
      "CREATE (n14:l4:l10:l7:l1:l0:l5 {id : 61});",
      "CREATE (n15 {id : 62, k9 : false, k3 : -230629454});",
      "CREATE (n16:l7:l10:l1:l0:l4:l8:l5:l6:l11:l9:l3 {id : 63});",
      "CREATE (n17:l3:l4:l5:l6 {id : 64});",
      "CREATE (n18:l0:l5:l2:l7:l8:l11:l9:l1:l10 {id : 68, k9 : false});",
      "CREATE (n19:l1:l3:l7:l4 {id : 69});",
      "CREATE (n20:l10:l1:l11:l3:l9:l8:l6:l5:l7 {id : 72, k9 : false, k3 : 1131434370});",
      "CREATE (n21:l4:l5:l6:l8:l1:l10:l7:l9:l2:l3:l11:l0 {id : 74});",
      "CREATE (n22:l3:l7:l5 {id : 77});",
      "CREATE (n23:l2:l8:l5 {id : 80});",
      "CREATE (n24:l8:l1:l6:l2:l3:l10:l0:l9:l4:l7:l5 {id : 81, k9 : false});",
      "CREATE (n25:l6:l3:l2:l5:l8:l11:l7:l9:l0:l1:l10:l4 {id : 85});",
      "CREATE (n26:l3:l6:l5 {id : 88, k9 : false, k3 : 494758125});",
      "CREATE (n27:l3:l7:l0 {id : 90});",
      "CREATE (n28:l2:l10:l9:l8:l3:l4:l1 {id : 91, k9 : false});",
      "CREATE (n29:l0:l2:l1:l5:l7:l4:l6:l9:l10 {id : 94});",
      "CREATE (n30:l3:l7:l8:l4:l10:l5:l6:l1:l0:l9:l2:l11 {id : 95, k9 : false, k3 : 359169101});",
      "CREATE (n31:l7:l10:l5:l11:l0:l1:l3:l4:l2 {id : 98});",
      "CREATE (n32:l3:l5:l11:l8:l2 {id : 99, k9 : false, k3 : 1016939459});",
      "CREATE (n33:l2:l9:l3:l4:l8 {id : 100});",
      "CREATE (n34:l11:l5:l0:l6:l7:l1 {id : 102, k9 : false, k3 : -1795763093});",
      "CREATE (n35:l5 {id : 104, k9 : false, k3 : -1132658883});",
      "CREATE (n36:l3 {id : 105});",
      "CREATE (n37:l9 {id : 108});",
      "CREATE (n38:l8:l1:l4:l3:l2:l7:l5:l11:l10:l0:l9 {id : 109, k9 : false});",
      "CREATE (n39:l8:l4:l1:l9:l0:l5:l11:l10 {id : 110, k9 : false, k3 : 1});",
      "CREATE (n40 {id : 113});",
      "CREATE (n41:l3:l11:l5:l7:l10:l2:l1:l8:l6 {id : 114, k9 : false});",
      "CREATE (n42:l8:l10:l11:l4:l2:l9:l6:l5:l0:l7 {id : 115, k2 : true, k7 : ['d', 'lSR', 'PBQdTI4', '9']});",
      "CREATE (n43:l11:l7:l6:l0 {id : 116});",
      "CREATE (n44:l9:l11:l6:l5:l1:l7:l8:l10 {id : 119});",
      "CREATE (n45:l2:l1:l0:l5:l9:l11 {id : 120});",
      "CREATE (n46:l5:l8:l3:l0:l2:l1:l6:l11:l9:l4 {id : 121, k9 : false, k3 : -1374122590, k2 : true, k7 : ['UKri', 'b', 'c', 'b']});",
      "CREATE (n47:l7:l0:l1:l11:l8:l2:l10 {id : 122, k9 : false, k3 : 0});",
      "CREATE (n48:l5:l0:l11:l2:l1:l6:l8:l9:l3:l4 {id : 123});",
      "CREATE (n49:l2:l1:l9 {id : 124, k9 : false, k3 : -2146844406});",
      "CREATE (n50:l11:l7:l4:l1:l2:l10:l3:l5:l8:l6:l0 {id : 125, k9 : false, k3 : -1688435024});",
      "CREATE (n51 {id : 127});",
      "MATCH (a {id:123}), (b {id:35}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:68}), (b {id:104}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:62}), (b {id:98}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:109}), (b {id:109}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:104}), (b {id:95}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:90}), (b {id:90}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:119}), (b {id:88}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:35}), (b {id:98}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:37}), (b {id:88}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:88}), (b {id:58}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:35}), (b {id:35}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:113}), (b {id:53}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:55}), (b {id:55}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:113}), (b {id:50}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:33}), (b {id:98}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:116}), (b {id:116}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:68}), (b {id:95}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:68}), (b {id:80}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:32}), (b {id:32}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:37}), (b {id:58}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:109}), (b {id:77}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:77}), (b {id:77}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:33}), (b {id:69}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:74}), (b {id:119}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:109}), (b {id:72}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:46}), (b {id:74}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:46}), (b {id:98}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:50}), (b {id:50}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:110}), (b {id:124}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:46}), (b {id:120}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:94}), (b {id:55}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:114}), (b {id:85}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:90}), (b {id:48}) CREATE (a)-[:rt5]->(b);",
      "MATCH (a {id:113}), (b {id:74}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:102}), (b {id:102}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:100}), (b {id:53}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:74}), (b {id:108}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:90}), (b {id:53}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:58}), (b {id:58}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:124}), (b {id:55}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:74}), (b {id:113}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:125}), (b {id:32}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:113}), (b {id:98}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:35}), (b {id:51}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:115}), (b {id:35}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:59}), (b {id:58}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:72}), (b {id:98}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:63}), (b {id:61}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:91}), (b {id:53}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:89}), (b {id:89}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:61}), (b {id:55}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:102}), (b {id:61}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:85}), (b {id:50}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:80}), (b {id:122}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:72}), (b {id:113}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:64}), (b {id:80}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:62}), (b {id:98}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:113}), (b {id:43}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:58}), (b {id:72}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:110}), (b {id:119}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:72}), (b {id:99}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:68}), (b {id:74}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:33}), (b {id:45}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:50}), (b {id:61}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:127}), (b {id:51}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:116}), (b {id:113}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:81}), (b {id:127}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:55}), (b {id:113}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:105}), (b {id:59}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:102}), (b {id:125}) CREATE (a)-[:rt8]->(b);",
      "MATCH (a {id:121}), (b {id:115}) CREATE (a)-[:rt1]->(b);",
      "MATCH (a {id:121}), (b {id:58}) CREATE (a)-[:rt11]->(b);"
  };

  private static final String MERGE_CLAUSE = """
      MERGE (n2:l1:l6:l7 {
        k0: false, k1: ['AJ1','\\''], k2: false, k4: 'I2', k5: [-2147483648],
        k11: false, k6: 'KvvnrFe', k10: false, k7: ['vuxUEI','yF2','L'],
        k8: true, k9: false, id: 128, klist: ['r']
      })
      ON MATCH SET n1.k2 = NULL
      """;

  private static final String OPTIONAL_CLAUSE = """
      OPTIONAL MATCH (n2), ({k7: ['d','lSR','PBQdTI4','9']})<-[]-(n4 {k2: true})-[r0:rt11|rt9]->(n3:l5 {k7: ['h']})
      WHERE toFloatOrNull(n0.k3) <> -432186678
      ORDER BY n0.id,n1.id,n2.id,n3.id,n4.id,r0.id,alias0
      RETURN {alias0: alias0, n0: n0, n1: n1, p0: p0, n2: n2, n4: n4, n3: n3, r0: r0} AS __layer_row""";

  private static String barrier(final int i) {
    return "FOREACH (elem" + i + " IN [] | CREATE (:BarrierSentinel))\n";
  }

  private static final String LEFT =
      "UNWIND [1,2] AS alias0\nMATCH p0 = (n0 {k9: false})-[:rt8|rt5*0..3]->(n1)\n" + MERGE_CLAUSE + OPTIONAL_CLAUSE;

  private static final String RIGHT =
      "UNWIND [1,2] AS alias0\n" + barrier(0) + "MATCH p0 = (n0 {k9: false})-[:rt8|rt5*0..3]->(n1)\n" + barrier(1) + MERGE_CLAUSE
          + barrier(2) + OPTIONAL_CLAUSE;

  @Override
  protected void beginTest() {
    load();
  }

  private void load() {
    for (final String statement : SETUP)
      database.command("opencypher", statement);
  }

  @Test
  void emptyForeachDoesNotChangeTheBindings() {
    final List<String> left = rows(LEFT);

    // MERGE writes, so the second shape runs on a rebuilt graph
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    load();
    final List<String> right = rows(RIGHT);

    assertThat(left).hasSize(288);
    assertThat(right).containsExactlyElementsOf(left);
    assertThat(database.getSchema().existsType("BarrierSentinel")).isFalse();
  }

  /**
   * The OPTIONAL MATCH reads {@code k2}, which the MERGE before it writes: clause by clause it sees every row's write.
   * The only {@code n4} able to reach an {@code n3} is node 121, and the path match makes it an {@code n1} (zero hops from
   * itself), so by the time the OPTIONAL MATCH runs its {@code k2} is gone and no row binds an {@code n4}.
   */
  @Test
  void optionalMatchSeesEveryWriteOfTheMergeBeforeIt() {
    final List<String> rows = rows(LEFT);
    assertThat(rows).hasSize(288);
    assertThat(rows).allMatch(row -> row.split("\\|", -1)[4].equals("null"));
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Map<String, Object> row = r.getProperty("__layer_row");
        rows.add(describe(row));
      }
    }
    return rows;
  }

  private static String describe(final Map<String, Object> row) {
    return row.get("alias0") + "|" + id(row.get("n0")) + "|" + id(row.get("n1")) + "|" + id(row.get("n2")) + "|" + id(row.get("n4"))
        + "|" + id(row.get("n3")) + "|" + (row.get("r0") == null ? null : ((Edge) row.get("r0")).getTypeName());
  }

  private static Object id(final Object vertex) {
    return vertex == null ? null : ((Vertex) vertex).get("id");
  }
}
