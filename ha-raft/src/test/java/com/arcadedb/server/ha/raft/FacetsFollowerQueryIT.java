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
import com.arcadedb.query.facet.AdvancedFacetsFixture;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
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
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(5);
    // the absolute budget alone: the default share of the matches (all of them) would patch this lag
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
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
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
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
      // computed here: what the empty statistics below tell apart from an answer parsed from the leader
      assertThat(rs.getFacets().executionStats()).as("the facets were computed on the follower").isNotEmpty();
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
   * Plan 5 Task 14: the plan 5 kinds (HIERARCHY, DISTINCT, STATS over LONG, DATETIME, FLOAT, SHORT and INTEGER properties, VALUE-ordered
   * terms with their cursor) answered by each follower from its own projection and hierarchy equal the leader's, and the answer a
   * follower forwards to the leader comes back through the leader-answer parser with the same members.
   */
  @Test
  @Timeout(300)
  void plan5KindsOnFollowersAndThroughTheLeaderAnswer() throws Exception {
    final int leader = requireLeaderIndex();
    AdvancedFacetsFixture.load(database(leader), 1805L, 500);
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      FacetOracle.awaitCaughtUp(database(i));

    for (final String query : List.of(AdvancedFacetsFixture.ASSET_QUERY, AdvancedFacetsFixture.MEASURE_QUERY)) {
      final FacetResults leaderFacets = AdvancedFacetsFixture.facets(database(leader), query);
      assertThat(leaderFacets.get(query.contains("Measure") ? "r" : "t").source()).isEqualTo("index");
      for (int i = 0; i < getServerCount(); i++)
        AdvancedFacetsFixture.assertSameValues("server " + i, AdvancedFacetsFixture.facets(database(i), query), leaderFacets);

      final int follower = firstFollowerOf(leader);
      try (final ResultSet rs = database(follower).command("sqlscript", query + ";")) {
        final FacetResults forwarded = rs.getFacets();
        assertThat(forwarded).as("a forwarded %s keeps the leader's facets", query).isNotNull();
        assertThat(forwarded.executionStats()).as("parsed from the leader's answer").isEmpty();
        // the leader-answer parser keeps the values as the JSON parse gives them, as it keeps a row's: the leader's HTTP encoding (a
        // date-time in the database's format), member for member
        final JSONObject leaderAnswer = new JSONObject(
            JsonSerializer.createJsonSerializer().serializeFacets(database(leader), leaderFacets).toString());
        leaderAnswer.remove("meta");
        assertThat(withoutMeta(forwarded)).isEqualTo(leaderAnswer.toString());
      }
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
