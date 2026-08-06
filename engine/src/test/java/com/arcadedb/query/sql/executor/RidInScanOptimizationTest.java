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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.partitioning.PartitioningTestFixture;

import org.junit.jupiter.api.Test;

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
