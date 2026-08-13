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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.partitioning.PartitioningTestFixture;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the planner rewrite that recognizes {@code WHERE @rid = <RID>} and
 * {@code WHERE @rid IN [<RID list>]} on a type target and plans them as a direct
 * {@link FetchFromRidsStep} (address-based, O(k)) instead of a {@link FetchFromTypeWithFilterStep}
 * full type scan with a per-record filter (O(n) in the size of the type). Issue #5824.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RidInScanOptimizationTest extends TestHelper {

  private RID doc0Rid;
  private RID doc1Rid;
  private RID doc2Rid;
  private RID otherTypeRid;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
    database.getSchema().createDocumentType("Other");

    database.transaction(() -> {
      final MutableDocument doc0 = database.newDocument("Doc").set("name", "doc0").set("active", true).save();
      final MutableDocument doc1 = database.newDocument("Doc").set("name", "doc1").set("active", false).save();
      final MutableDocument doc2 = database.newDocument("Doc").set("name", "doc2").set("active", true).save();

      doc0Rid = doc0.getIdentity();
      doc1Rid = doc1.getIdentity();
      doc2Rid = doc2.getIdentity();

      otherTypeRid = database.newDocument("Other").set("name", "other0").save().getIdentity();
    });

    // The type creations above invalidate the execution plan cache, stamping
    // ExecutionPlanCache.lastInvalidation with System.currentTimeMillis(). SelectExecutionPlanner's
    // cache-put guard (createExecutionPlan) rejects a plan whose own planningStart timestamp isn't
    // strictly greater than that invalidation stamp. On a fast/warmed-up JVM the setup above and a
    // test's first query can land in the same millisecond tick, so a plan that is legitimately built
    // after the invalidation gets skipped anyway - this only matters here because #5855 is what makes
    // these @rid plans cacheable in the first place. Not a bug in the fix; just avoid racing the clock.
    final long setupMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() == setupMillis)
      Thread.onSpinWait();
  }

  @Test
  void planUsesFetchFromRidsStepForEquality() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid = " + doc1Rid);
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(findStep(plan, FetchFromRidsStep.class)).as("@rid = <RID> must use FetchFromRidsStep").isNotNull();
    assertThat(findStep(plan, FetchFromTypeWithFilterStep.class)).as("must not fall back to a full scan").isNull();
    assertThat(findStep(plan, FetchFromTypeExecutionStep.class)).as("must not fall back to a full scan").isNull();

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("doc1");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void planUsesFetchFromRidsStepForBracketInList() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN [" + doc0Rid + ", " + doc2Rid + "]");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(findStep(plan, FetchFromRidsStep.class)).as("@rid IN [...] must use FetchFromRidsStep").isNotNull();
    assertThat(findStep(plan, FetchFromTypeWithFilterStep.class)).isNull();

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void planUsesFetchFromRidsStepForParenthesizedInList() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN (" + doc0Rid + ", " + doc2Rid + ")");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(findStep(plan, FetchFromRidsStep.class)).as("@rid IN (...) must use FetchFromRidsStep").isNotNull();

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void planUsesFetchFromRidsStepForBoundParameterList() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN ?", List.of(doc0Rid, doc1Rid));
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(findStep(plan, FetchFromRidsStep.class)).as("@rid IN ? bound to a RID list must use FetchFromRidsStep").isNotNull();

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc1");
  }

  @Test
  void ridInListCombinesWithAdditionalFilter() {
    final ResultSet rs = database.query("sql",
        "SELECT FROM Doc WHERE @rid IN [" + doc0Rid + ", " + doc1Rid + ", " + doc2Rid + "] AND active = true");

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void ridInListWithDuplicateRidDoesNotDuplicateTheRow() {
    // A scan evaluates "@rid IN [...]" as a per-record membership test: a repeated value in the
    // list must not visit the matching record twice.
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN [" + doc0Rid + ", " + doc0Rid + "]");

    assertThat(collectNames(rs)).containsExactly("doc0");
  }

  @Test
  void ridInListExcludesRidsOfAnotherType() {
    // otherTypeRid belongs to type "Other": the FilterByTypeStep chained after FetchFromRidsStep
    // must drop it, matching what a plain type scan would have returned.
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN [" + doc0Rid + ", " + otherTypeRid + "]");

    assertThat(collectNames(rs)).containsExactly("doc0");
  }

  @Test
  void ridInListSkipsNonExistentRidsWithoutError() {
    final RID deleted = doc1Rid;
    database.transaction(() -> database.lookupByRID(deleted, true).asDocument().delete());

    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN [" + doc0Rid + ", " + deleted + ", " + doc2Rid + "]");

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void ridInEmptyBoundParameterListReturnsNoRows() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN ?", List.of());
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void ridInNullBoundParameterReturnsNoRows() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN ?", new Object[] { null });
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void ridWithOrConditionFallsBackToScanButStaysCorrect() {
    // The optimization only applies to a single AND-block; an OR at the top level must still be
    // answered correctly by the regular scan path.
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid IN [" + doc0Rid + "] OR name = 'doc2'");

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void ridNotInFallsBackToScanButStaysCorrect() {
    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE @rid NOT IN [" + doc1Rid + "]");

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void ridInListWithRemainingLetReferencingConditionStaysCorrect() {
    // The remaining (non-@rid) AND condition references a per-record LET variable, which is only
    // computed after the fetch. A rewrite that pushes this remaining condition into the fetch-side
    // plan (ahead of LET) would silently drop every row - see LetWherePredicatePushdownTest for the
    // same hazard on the generic predicate-pushdown path.
    final ResultSet rs = database.query("sql",
        "SELECT name, $flag AS flag FROM Doc LET $flag = (active = true) WHERE @rid IN ["
            + doc0Rid + ", " + doc1Rid + ", " + doc2Rid + "] AND $flag = true");

    assertThat(collectNames(rs)).containsExactlyInAnyOrder("doc0", "doc2");
  }

  @Test
  void ridInListIgnoresPartitionPruningAndReturnsRidsFromAnyBucket() {
    // RID-address fetch bypasses the partition-pruning bucket narrowing entirely (it doesn't need
    // it: the RID already encodes its physical bucket). Confirm records from different partitions
    // are all still resolved correctly when named directly by RID.
    PartitioningTestFixture.createPartitionedDocType(database, "PartDoc", 4, false);
    PartitioningTestFixture.populateDocs(database, "PartDoc", false);

    final List<RID> allRids = new java.util.ArrayList<>();
    final ResultSet all = database.query("sql", "SELECT @rid AS r FROM PartDoc");
    while (all.hasNext())
      allRids.add(all.next().<RID>getProperty("r"));
    all.close();
    assertThat(allRids).hasSize(4); // one per tenant, spread across the 4 partitioned buckets

    final String ridList = allRids.stream().map(RID::toString).reduce((a, b) -> a + ", " + b).orElseThrow();
    final ResultSet rs = database.query("sql", "SELECT tenant_id FROM PartDoc WHERE @rid IN [" + ridList + "]");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
    assertThat(findStep(plan, FetchFromRidsStep.class)).isNotNull();

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    rs.close();
    assertThat(count).isEqualTo(4);
  }

  @Test
  void ridEqualityBoundParameterPlanIsCacheableAndReresolvesOnReuse() {
    // Issue #5855: before this fix, FetchFromRidsStep baked the resolved RID into the step at
    // plan-build time and never overrode canBeCached()/copy(), so the whole plan was never put in
    // db.getExecutionPlanCache(). A "get by id" query executed repeatedly with different bound
    // RIDs must re-plan from scratch every time it happened.
    final String sql = "SELECT FROM Doc WHERE @rid = ?";
    final DatabaseInternal db = (DatabaseInternal) database;

    final ResultSet rs1 = database.query("sql", sql, doc0Rid);
    final ExecutionPlan plan1 = rs1.getExecutionPlan().orElseThrow();
    assertThat(findStep(plan1, FetchFromRidsStep.class)).isNotNull();
    assertThat(((InternalExecutionPlan) plan1).canBeCached()).as("plan built for @rid = ? must now be cacheable").isTrue();
    assertThat(collectNames(rs1)).containsExactly("doc0");

    assertThat(db.getExecutionPlanCache().contains(sql)).as("plan must actually be in the execution plan cache").isTrue();

    // Same statement text, a DIFFERENT bound RID: a stale cached plan would either return doc0
    // again (the RID baked in from the first build) or nothing at all.
    final ResultSet rs2 = database.query("sql", sql, doc1Rid);
    assertThat(collectNames(rs2)).containsExactly("doc1");

    final ResultSet rs3 = database.query("sql", sql, doc2Rid);
    assertThat(collectNames(rs3)).containsExactly("doc2");
  }

  @Test
  void ridInListBoundParameterPlanIsCacheableAndReresolvesOnReuse() {
    final String sql = "SELECT FROM Doc WHERE @rid IN ?";
    final DatabaseInternal db = (DatabaseInternal) database;

    final ResultSet rs1 = database.query("sql", sql, List.of(doc0Rid, doc1Rid));
    final ExecutionPlan plan1 = rs1.getExecutionPlan().orElseThrow();
    assertThat(findStep(plan1, FetchFromRidsStep.class)).isNotNull();
    assertThat(((InternalExecutionPlan) plan1).canBeCached()).isTrue();
    assertThat(collectNames(rs1)).containsExactlyInAnyOrder("doc0", "doc1");

    assertThat(db.getExecutionPlanCache().contains(sql)).isTrue();

    // Reused plan, bound to a completely different RID list.
    final ResultSet rs2 = database.query("sql", sql, List.of(doc2Rid));
    assertThat(collectNames(rs2)).containsExactly("doc2");
  }

  @Test
  void ridInListWithOneUnresolvableElementStillMatchesTheResolvableOnesOnCacheReuse() {
    // A cache-hit re-execution has no scan fallback left (unlike the build-time shape check in
    // SelectExecutionPlanner.extractRidEqualityOrInList): one bad element in an otherwise-valid list
    // must not silently drop the matches that ARE real RIDs.
    final String sql = "SELECT FROM Doc WHERE @rid IN ?";
    final DatabaseInternal db = (DatabaseInternal) database;

    // First execution: every element resolves, so the plan is built AND cached as FetchFromRidsStep.
    final ResultSet rs1 = database.query("sql", sql, List.of(doc0Rid, doc1Rid));
    final ExecutionPlan plan1 = rs1.getExecutionPlan().orElseThrow();
    assertThat(findStep(plan1, FetchFromRidsStep.class)).isNotNull();
    assertThat(collectNames(rs1)).containsExactlyInAnyOrder("doc0", "doc1");
    assertThat(db.getExecutionPlanCache().contains(sql)).isTrue();

    // Second execution: cache hit, reuses the exact same FetchFromRidsStep - one element is not a
    // RID at all. The record matching the other, valid element must still come back.
    final ResultSet rs2 = database.query("sql", sql, Arrays.asList(doc2Rid, "not-a-rid"));
    assertThat(collectNames(rs2)).containsExactly("doc2");
  }

  @Test
  void ridInListEmptyThenNonEmptyBoundParameterAcrossCachedReuseStaysCorrect() {
    // Pins the EmptyStep-caching hazard: the old code short-circuited an empty resolved RID list to
    // a non-cacheable EmptyStep at build time. The new code must never bake "this execution's RIDs
    // happen to be empty" into a plan that a later, differently-bound execution can reuse.
    final String sql = "SELECT FROM Doc WHERE @rid IN ?";
    final DatabaseInternal db = (DatabaseInternal) database;

    final ResultSet rs1 = database.query("sql", sql, List.of());
    assertThat(rs1.hasNext()).isFalse();
    rs1.close();

    assertThat(db.getExecutionPlanCache().contains(sql)).as("even a build that resolves to zero RIDs must be cacheable").isTrue();

    final ResultSet rs2 = database.query("sql", sql, List.of(doc0Rid));
    assertThat(collectNames(rs2)).containsExactly("doc0");
  }

  @Test
  void ridEqualityBoundParameterCombinesWithRemainingFilterAcrossCachedReuse() {
    final String sql = "SELECT FROM Doc WHERE @rid = ? AND active = true";
    final DatabaseInternal db = (DatabaseInternal) database;

    // doc0 is active: matches.
    final ResultSet rs1 = database.query("sql", sql, doc0Rid);
    assertThat(collectNames(rs1)).containsExactly("doc0");
    assertThat(db.getExecutionPlanCache().contains(sql)).isTrue();

    // doc1 is NOT active: the remaining filter chained after the reused, cached FetchFromRidsStep
    // must still be re-evaluated correctly.
    final ResultSet rs2 = database.query("sql", sql, doc1Rid);
    assertThat(rs2.hasNext()).isFalse();
    rs2.close();
  }

  private static List<String> collectNames(final ResultSet rs) {
    final List<String> names = new java.util.ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    rs.close();
    return names;
  }

  private static <T extends ExecutionStep> T findStep(final ExecutionPlan plan, final Class<T> type) {
    for (final ExecutionStep step : plan.getSteps())
      if (type.isInstance(step))
        return type.cast(step);
    return null;
  }
}
