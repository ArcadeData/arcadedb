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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for issue #6665: on ArcadeDB 26.8.1, a read-only {@code CALL (...) { ... RETURN count(*) }}
 * subquery reported a different {@code count(*)} depending on whether its inner {@code MATCH}/{@code WHERE}
 * result flowed straight into {@code RETURN count(*)} or was first materialized through {@code collect()}/
 * {@code UNWIND} before the same {@code count(*)} - two forms that must be equivalent for a read-only subquery.
 * <p>
 * This exact repro (verbatim from the issue) no longer reproduces on this branch: both forms agree with each
 * other and with a ground-truth standalone count of the same pattern. This test pins that down so a future
 * regression in CALL-subquery / {@code count(*)} evaluation is caught.
 */
class Issue6665CallSubqueryCountEquivalenceTest {

  @Test
  void directCountAndMaterializedCountAgreeInsideCallSubquery() {
    final Database database = new DatabaseFactory("./target/databases/issue6665-call-count-equivalence").create();
    try {
      database.transaction(() -> database.command("cypher", """
          CREATE (oa:V {klist: [1167157462, -165975848, -164122097]}),
                 (ob:V {k10: 254370155}),
                 (oc:V),
                 (od:V {k2: "CILs3s"}),
                 (oe:V {k10: -866288821}),
                 (n0:V {k7: 1994572697, k2: "D5ZqpQB"}),
                 (n1:V),
                 (x:V {k10: -1192051958}),
                 (y:V {k9: "5sm", id: 85}),
                 (ob)-[:rt6 {k8: "G", k3: true}]->(oa),
                 (ob)-[:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(oc),
                 (od)-[:rt9 {k7: -51638479}]->(oe),
                 (n1)-[:R]->(x)-[:rt8]->(y)-[:rt2 {k11: [1]}]->(n0)
          """));

      // Query A: the CALL subquery's inner MATCH/WHERE result flows straight into RETURN count(*).
      final String queryA = """
          MATCH DIFFERENT RELATIONSHIPS
            ({klist: [1167157462, -165975848, -164122097]})
              <-[r0:rt6 {k8: "G"}]-({k10: 254370155})
              -[r1:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(),
            p0 = ({k2: "CILs3s"})-[:rt9 {k7: -51638479}]->({k10: -866288821})
          WHERE r0.k3 = true
          CALL (r0, r1, p0) {
            MATCH (n0 {k7: 1994572697, k2: "D5ZqpQB"}),
                  (n1)-[r3]->({k10: -1192051958})-[:rt8]->
                    (:V {k9: "5sm", id: 85})-[r2:rt2|rt10|rt9]->(n0)
            WHERE single(item IN r2.k11 WHERE item IS NOT NULL)
            RETURN count(*) AS __expr_count
          }
          RETURN __expr_count AS __v
          """;

      // Query B: identical, but the matched bindings are first materialized through collect()/UNWIND before the
      // same count(*) - a read-only subquery must report the same answer either way.
      final String queryB = """
          MATCH DIFFERENT RELATIONSHIPS
            ({klist: [1167157462, -165975848, -164122097]})
              <-[r0:rt6 {k8: "G"}]-({k10: 254370155})
              -[r1:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(),
            p0 = ({k2: "CILs3s"})-[:rt9 {k7: -51638479}]->({k10: -866288821})
          WHERE r0.k3 = true
          CALL (r0, r1, p0) {
            MATCH (n0 {k7: 1994572697, k2: "D5ZqpQB"}),
                  (n1)-[r3]->({k10: -1192051958})-[:rt8]->
                    (:V {k9: "5sm", id: 85})-[r2:rt2|rt10|rt9]->(n0)
            WHERE single(item IN r2.k11 WHERE item IS NOT NULL)
            WITH {r0: r0, r1: r1, p0: p0, n0: n0, n1: n1, r3: r3, r2: r2} AS __row
            WITH collect(__row) AS __rows
            UNWIND __rows AS __layer_row
            RETURN count(*) AS __expr_count
          }
          RETURN __expr_count AS __v
          """;

      final long resultA;
      try (ResultSet rs = database.query("cypher", queryA)) {
        assertThat(rs.hasNext()).isTrue();
        resultA = ((Number) rs.next().getProperty("__v")).longValue();
      }
      final long resultB;
      try (ResultSet rs = database.query("cypher", queryB)) {
        assertThat(rs.hasNext()).isTrue();
        resultB = ((Number) rs.next().getProperty("__v")).longValue();
      }

      // Ground truth: the same inner pattern, matched standalone (bound to r0/r1/p0 via WITH instead of CALL) and
      // counted directly, independent of the CALL-subquery machinery under test.
      final String groundTruth = """
          MATCH DIFFERENT RELATIONSHIPS
            ({klist: [1167157462, -165975848, -164122097]})
              <-[r0:rt6 {k8: "G"}]-({k10: 254370155})
              -[r1:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(),
            p0 = ({k2: "CILs3s"})-[:rt9 {k7: -51638479}]->({k10: -866288821})
          WHERE r0.k3 = true
          WITH r0, r1, p0
          MATCH (n0 {k7: 1994572697, k2: "D5ZqpQB"}),
                (n1)-[r3]->({k10: -1192051958})-[:rt8]->
                  (:V {k9: "5sm", id: 85})-[r2:rt2|rt10|rt9]->(n0)
          WHERE single(item IN r2.k11 WHERE item IS NOT NULL)
          RETURN count(*) AS c
          """;
      final long groundTruthCount;
      try (ResultSet rs = database.query("cypher", groundTruth)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        groundTruthCount = ((Number) row.getProperty("c")).longValue();
      }

      assertThat(groundTruthCount).as("the inner pattern matches exactly one row").isEqualTo(1L);
      assertThat(resultA).as("direct count(*) must match the ground-truth count").isEqualTo(groundTruthCount);
      assertThat(resultB).as("collect()/UNWIND-materialized count(*) must match the ground-truth count").isEqualTo(groundTruthCount);
      assertThat(resultA).as("direct and materialized count(*) must agree").isEqualTo(resultB);
    } finally {
      database.drop();
    }
  }
}
