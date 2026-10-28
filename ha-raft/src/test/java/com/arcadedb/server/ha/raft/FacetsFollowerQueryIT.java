/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code SELECT ... FACETS} answered by Raft followers (plan 4, Task 18): each node counts from its own projection, fed by the Raft
 * apply; a follower whose projection is held behind the replicated records patches the lag from the records and says so in
 * {@code meta}; a statement a follower forwards to the leader keeps the leader's facets.
 */
class FacetsFollowerQueryIT extends ColumnarHATestSupport {
  private static final int    RECORDS = 3_000;
  private static final String QUERY   =
      "SELECT FROM " + ColumnarTestData.TYPE + " FACETS b: brand TERMS TOP 1000, t: tags TERMS TOP 1000";

  @Test
  @Timeout(300)
  void followersAndLeaderReturnEqualCountsAfterConvergence() throws Exception {
    final int leader = requireLeaderIndex();
    load(leader, 1801L);

    final String leaderFacets = withoutMeta(facets(leader));
    for (int i = 0; i < getServerCount(); i++) {
      assertThat(countOn(i)).as("server %d holds every record", i).isEqualTo(RECORDS);
      final FacetResults facets = facets(i);
      assertThat(facets.exact()).as("server %d answers exactly", i).isTrue();
      assertThat(facets.patched()).as("server %d patches nothing once caught up", i).isZero();
      assertThat(facets.lagMs()).as("server %d does not lag once caught up", i).isZero();
      assertThat(facets.get("b").source()).isEqualTo("index");
      assertThat(withoutMeta(facets)).as("server %d answers the leader's facets", i).isEqualTo(leaderFacets);
      FacetOracle.assertFacetsEqualGroupBy(database(i), ColumnarTestData.TYPE, "brand", null);
      FacetOracle.assertFacetsEqualGroupBy(database(i), ColumnarTestData.TYPE, "tags", null);
    }
  }

  @Test
  @Timeout(300)
  void followerReportsLagAndStaysExactWhilePaused() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final List<RID> rids = load(leader, 1802L);

    try (final AutoCloseable hold = manager(follower).suspendApply()) {
      writeWhileHeld(leader, follower, rids);

      final FacetResults facets = awaitLagOn(follower);
      assertThat(facets.exact()).as("the patched follower answers exactly").isTrue();
      assertThat(facets.patched()).as("the follower read the records its projection has not applied").isPositive();
      FacetOracle.assertFacetsEqualGroupBy(database(follower), ColumnarTestData.TYPE, "brand", null);
      FacetOracle.assertFacetsEqualGroupBy(database(follower), ColumnarTestData.TYPE, "tags", null);
    }

    awaitCaughtUpOn(follower);
    final FacetResults caughtUp = facets(follower);
    assertThat(caughtUp.patched()).isZero();
    assertThat(caughtUp.lagMs()).isZero();
    assertThat(withoutMeta(caughtUp)).isEqualTo(withoutMeta(facets(leader)));
  }

  @Test
  @Timeout(300)
  void followerOverPatchBudgetReportsInexact() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final List<RID> rids = load(leader, 1803L);

    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(5);
    try {
      try (final AutoCloseable hold = manager(follower).suspendApply()) {
        writeWhileHeld(leader, follower, rids);

        final FacetResults facets = awaitLagOn(follower);
        assertThat(facets.exact()).as("over the patch budget the follower counts from its lagging projection").isFalse();
        assertThat(facets.patched()).as("over the patch budget nothing is patched").isZero();
      }

      awaitCaughtUpOn(follower);
      await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
        final FacetResults facets = facets(follower);
        assertThat(facets.exact()).isTrue();
        assertThat(facets.patched()).isZero();
        assertThat(facets.lagMs()).isZero();
      });
      FacetOracle.assertFacetsEqualGroupBy(database(follower), ColumnarTestData.TYPE, "brand", null);
    } finally {
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
    }
  }

  @Test
  @Timeout(300)
  void forwardedCommandOnFollowerKeepsFacets() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    load(leader, 1804L);
    final String leaderFacets = withoutMeta(facets(leader));

    // a read-only SQL command runs on the follower itself
    try (final ResultSet rs = database(follower).command("sql", QUERY)) {
      assertThat(rs.getFacets()).as("a SELECT ... FACETS command on a follower returns facets").isNotNull();
      assertThat(withoutMeta(rs.getFacets())).isEqualTo(leaderFacets);
    }

    // a script is executed by the leader: its answer comes back through the leader-answer parser
    try (final ResultSet rs = database(follower).command("sqlscript", QUERY + ";")) {
      final FacetResults facets = rs.getFacets();
      assertThat(facets).as("a forwarded SELECT ... FACETS keeps the leader's facets").isNotNull();
      assertThat(facets.executionStats()).as("the facets were parsed from the leader's answer, not computed here").isEmpty();
      assertThat(withoutMeta(facets)).isEqualTo(leaderFacets);
    }
  }

  /**
   * The schema and {@link #RECORDS} records written through the leader, every node caught up. A tag vocabulary within {@code TOP 1000}
   * keeps the comparison complete.
   */
  private List<RID> load(final int leader, final long seed) throws InterruptedException {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.tagVocabulary = 300;
    data.createSchema(database(leader));
    final List<RID> rids = data.insertAssets(database(leader), RECORDS);
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      FacetOracle.awaitCaughtUp(database(i));
    return rids;
  }

  /** 200 brand updates then 100 inserts on the leader, awaited on the held follower (the inserts come last in the Raft log). */
  private void writeWhileHeld(final int leader, final int follower, final List<RID> rids) throws InterruptedException {
    final Database leaderDb = database(leader);
    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++)
        leaderDb.lookupByRID(rids.get(i * 13), true).asDocument().modify().set("brand", "brand-" + i % 7).save();
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++)
        leaderDb.newDocument(ColumnarTestData.TYPE).set("brand", "brand-" + i % 5).set("tags", List.of("tag-" + i % 11))
            .set("fileSize", 1024L * i).save();
    });
    await().atMost(60, TimeUnit.SECONDS).until(() -> countOn(follower) == RECORDS + 100);
    assertThat(awaitReplicationIsCompleted(follower)).isTrue();
  }

  /** The follower's facets once its held projection reports a lag (the lag is measured in whole milliseconds). */
  private FacetResults awaitLagOn(final int follower) {
    final AtomicReference<FacetResults> facets = new AtomicReference<>();
    await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
      facets.set(facets(follower));
      assertThat(facets.get().lagMs()).as("the held follower reports its lag").isPositive();
    });
    return facets.get();
  }

  private FacetResults facets(final int serverIndex) {
    try (final ResultSet rs = database(serverIndex).query("sql", QUERY)) {
      final FacetResults facets = rs.getFacets();
      assertThat(facets).as("server %d returns facets", serverIndex).isNotNull();
      return facets;
    }
  }

  private long countOn(final int serverIndex) {
    return database(serverIndex).countType(ColumnarTestData.TYPE, true);
  }

  private static String withoutMeta(final FacetResults facets) {
    final JSONObject json = facets.toJSON();
    json.remove("meta");
    return json.toString();
  }
}
