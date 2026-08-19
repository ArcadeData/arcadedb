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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

/**
 * End-to-end wire test for issue #6367: a small self-loop {@code CREATE}/{@code MERGE} autocommit
 * query, sent verbatim over Bolt with a real neo4j driver session and consumed with
 * {@code .consume()} (a lazy, unconsumed result can return success before the server would have
 * reported the failure - see the issue).
 * <p>
 * This is the literal reproduction from the issue report, reduced only by inlining the setup
 * statements as separate autocommit {@code session.run(...)} calls exactly as the reporter ran
 * them. Against ArcadeDB 26.8.1 (the version reported) this reliably fails with
 * {@code Neo.TransientError.Transaction.DeadlockDetected} - verified against the {@code 26.8.1} tag
 * as part of resolving this issue. It does not reproduce on this branch: the OpenCypher planner
 * and executor rewrite that landed between 26.8.1 and this branch already changed how this
 * self-loop pattern is planned and committed. This test locks that behavior in as a permanent
 * regression guard, since the query is a valuable, hard-to-construct-by-hand edge case (multiple
 * relationship legs bound to the same node within one {@code CREATE}, followed by a {@code MERGE}
 * that also self-loops and carries {@code ON CREATE}/{@code ON MATCH SET} touching a variable
 * bound by the earlier {@code CREATE}).
 * <p>
 * See also {@link com.arcadedb.query.opencypher.Issue6367MergeStepAutoRetryTest} in the engine
 * module, which pins down and regression-tests the specific defect this investigation found still
 * live on this branch: {@code MergeStep}, unlike {@code CreateStep}, did not retry an MVCC conflict
 * when it owned its own auto-commit mini-transaction (the normal shape for an unwrapped autocommit
 * caller such as Bolt's {@code handleRun}).
 */
public class Bolt6367SelfLoopMergeIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void selfLoopCreateThenMergeAutocommitsWithoutDeadlockDetected() {
    try (final Driver driver = getDriver()) {
      try (final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        s.run("CREATE (n0:l10:l2:l7:l1:l11:l0:l6 {" +
            "  k1: true, k3: 'A', k5: false, k11: 920049656, id: 1," +
            "  k6: 941632748, k7: 347700282, k8: 1294887099," +
            "  klist: [-713652930, 1014348474, 845078701]" +
            "})").consume();
        s.run("CREATE (n0 {" +
            "  k1: false, k2: 1310638743, k4: false, id: 2," +
            "  k6: 433137854, k10: [], k8: -478006707, k9: 'yh'" +
            "})").consume();
        s.run("CREATE (n0:l1:l4:l9:l3:l6:l5 {" +
            "  k1: false, k3: 'jJF', k4: true, k5: true, k11: -1580869030," +
            "  id: 3, k6: -1035541070, k10: ['P', 'K'], k7: -1778036611," +
            "  k8: -1313317333, klist: ['1', 'L']" +
            "})").consume();
        s.run("CREATE (n0:l11:l8:l6:l3:l7:l0:l2:l9:l1 {" +
            "  k0: 1201579394, k1: true, k2: 1171701452, k3: 'K', k5: false," +
            "  k11: -1477125454, id: 4, k6: -1655158678, k8: 358041376," +
            "  k9: 'CkLeqUD'" +
            "})").consume();
        s.run("CREATE (n0:l1:l2:l8:l7:l10:l5:l3:l9:l11 {" +
            "  k0: -1574558185, k1: false, k2: -1095456589, k3: 'i', k4: true," +
            "  k11: 947532216, id: 5, k6: 1593575670," +
            "  k10: [283091062, 1645515029, -417823137, 907835534], klist: []" +
            "})").consume();
        s.run("CREATE (n0:l8:l3:l9:l11 {" +
            "  k0: -890530952, k1: false, k2: 844390616, k3: '7', k4: false," +
            "  id: 7, k6: 1525525060, k10: [-910111137], klist: ['j']" +
            "})").consume();
        s.run("CREATE (n0:l9:l7:l2:l8:l0:l11:l4:l6 {" +
            "  k2: 878417539, k3: '06', k4: false, k5: true, id: 9," +
            "  k10: ['Es', 'L', 'U'], k8: -509574264, klist: ['AxYtys', 'Vf']" +
            "})").consume();
        s.run("CREATE (n0:l7:l1:l2 {" +
            "  k2: -441719403, k3: 'y', id: 10, k10: [1489714817]," +
            "  k7: 562337684, k8: 957538390, klist: ['qZU', 'R']" +
            "})").consume();
        s.run("CREATE (n0:l6:l2:l8:l10:l0 {" +
            "  k0: 1826277743, k2: -686971766, k4: true, k11: -1047952844," +
            "  id: 12, k6: 697361730, k8: -2916245, k9: 'J'," +
            "  klist: [-533643435]" +
            "})").consume();
        s.run("CREATE (n0:l5:l1:l2:l3 {" +
            "  k1: false, k2: 934023812, k3: 'F', k5: true, k11: 1311330458," +
            "  id: 35, k6: 283780461, k10: ['RT4Iu', 'u'], k7: -1702420027," +
            "  k8: -2051126576, k9: 'SD', klist: ['H', 'W48', 'UV', 'J']" +
            "})").consume();
        s.run("CREATE (n0:l4:l7:l6:l11:l3:l2:l5:l0:l1:l10:l8 {" +
            "  k0: -1767670436, k1: false, k2: -1725362774, k3: 'zGAVip'," +
            "  k4: false, k5: true, id: 54, k6: 855115655," +
            "  k10: [1188739270, -1501661906, -1728775435], k7: -904347188," +
            "  k8: -863276389, klist: [398671376, 319567150, 1443229443]" +
            "})").consume();
        s.run("CREATE (n0:l7:l11:l5:l10:l2:l3:l1:l0:l8:l9:l6:l4 {" +
            "  k2: 626672548, k3: 'h', k4: false, k5: false, k11: 662836309," +
            "  id: 117, k6: 283147199, k10: [-872739709, 241244914, 187549187]," +
            "  k7: -348270927, k9: 'x', klist: []" +
            "})").consume();
        s.run("MATCH (n0 {id: 54}), (n1 {id: 117}) " +
            "CREATE (n0)-[:rt4 {" +
            "  k0: 1423777216, k1: false, k2: 2012621405, k3: 'LF', k5: true," +
            "  k11: -1655318649, k6: -1447151942, k10: ['CojBI']," +
            "  k7: 1658684060, k8: -490348148, k9: 'N', id: 140," +
            "  klist: ['0', 'Hlvnq50Pn']" +
            "}]->(n1)").consume();
        s.run("MATCH (n0 {id: 35}), (n1 {id: 117}) " +
            "CREATE (n0)-[:rt9 {" +
            "  k0: 1829029150, k1: false, k2: 1958699186, k3: '7KS', k4: true," +
            "  k5: false, k11: 171376015, k6: 1471679659, k10: [1013404002]," +
            "  k7: 2038459859, k8: -536259914, k9: 'Da', id: 163," +
            "  klist: [-1601533447, 692060217, -273264309, -1769592516]" +
            "}]->(n1)").consume();

        final String query = "MATCH (n0 {id: 117}) " +
            "UNWIND n0.k10 AS alias0 " +
            "CREATE p0 = (n0) " +
            "  <-[:rt0 {" +
            "    k0: -374799104, k1: true, k2: 1001023838, k3: \"vNcYpJqkU\"," +
            "    k4: false, k5: false, k11: -1402383927, k6: -674816318," +
            "    k10: [1245313374], k7: 770821783, k8: -647852269, k9: \"D\"," +
            "    id: 606, klist: [1553210597, -1323333515]" +
            "  }]- (n0) " +
            "  <-[r0:rt2 {" +
            "    k0: 1742067710, k1: true, k3: \"D\", k4: true, k5: true," +
            "    k11: 1706560410, id: 607, k6: -1540121268, k7: -247756200," +
            "    k8: 1130553656, k9: \"e\", klist: [\"KPWmvP\", \"f\"]" +
            "  }]- (n0) " +
            "MERGE p1 = (n0) " +
            "  <-[r1:rt3 {" +
            "    k0: -855083000, k1: true, k2: 304240649, k3: \"Q\", k4: true," +
            "    k11: -1836187272, k6: 1924002997, k10: [-330464047, -2085991393]," +
            "    k7: 416264311, k8: 1771875405, k9: \"2Y\", id: 608, klist: []" +
            "  }]- (n0) " +
            "ON CREATE SET n0.k10 = NULL, r0.k0 = NULL, r0.k6 = -344491733 " +
            "ON MATCH SET n0.klist = ['OK', '8ve4q', 'p5tHJs'] " +
            "RETURN n0, r0, r1, p0, p1, alias0 " +
            "SKIP 8 LIMIT 0";

        // Pre-fix on 26.8.1: throws org.neo4j.driver.exceptions.TransientException with code
        // Neo.TransientError.Transaction.DeadlockDetected. On this branch: completes normally.
        s.run(query).consume();
      }
    }
  }
}
