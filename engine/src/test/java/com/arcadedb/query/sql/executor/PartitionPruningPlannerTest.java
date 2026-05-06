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
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the partition-aware bucket-pruning rule in {@link SelectExecutionPlanner}: when the type
 * uses {@link PartitionedBucketSelectionStrategy} and the WHERE clause binds the partition
 * property to a literal, the resulting plan must only scan the bucket(s) the strategy's hash
 * routes those values to. When the type's {@code needsRepartition} flag is set, the rule is
 * suppressed and queries fan out across every bucket. Issue #4087.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionPruningPlannerTest extends TestHelper {

  private static final String TYPE_NAME = "PartDoc";
  private static final int    BUCKETS   = 4;

  @Test
  void whereOnNonPartitionPropertyForcesFanOut() {
    // Use a non-indexed predicate so the planner picks the FetchFromTypeWithFilterStep path
    // (the index path ignores filterClusters today; pruning visible only via this path).
    createPartitionedType();
    populate();

    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE payload = 'p-acme'");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
    final FetchFromTypeWithFilterStep fetch = findFetcher(plan);

    assertThat(fetch).as("non-indexed WHERE must produce a FetchFromTypeWithFilterStep").isNotNull();
    assertThat(fetch.getSubSteps()).as("WHERE on non-partition property must scan every bucket").hasSize(BUCKETS);

    int count = 0;
    while (rs.hasNext()) {
      assertThat(rs.next().<String>getProperty("payload")).isEqualTo("p-acme");
      count++;
    }
    rs.close();
    assertThat(count).isEqualTo(1);
  }

  @Test
  void resultsCorrectWithFlagSetSuppressingPruning() {
    // With needsRepartition=true the planner must NOT prune. Records remain reachable.
    createPartitionedType();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);
    try {
      for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
        final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = '" + tenant + "'");
        assertThat(rs.hasNext()).as("tenant '" + tenant + "' must remain findable when pruning is suppressed").isTrue();
        assertThat(rs.next().<String>getProperty("tenant_id")).isEqualTo(tenant);
        rs.close();
      }
    } finally {
      type.setNeedsRepartition(false);
    }
  }

  @Test
  void literalQueryNarrowsIndexFilterBuckets() {
    // The unique index on tenant_id covers `WHERE tenant_id = 'X'` so the planner uses the
    // index path. The partition-pruning rule narrows {@code filterClusters}, which then becomes
    // the {@code filterBucketIds} on {@link GetValueFromIndexEntryStep}. Confirm that with the
    // flag false and a literal predicate, the step sees a single bucket id (the partition
    // target). With the flag true (or with parameter binding) the step sees every bucket - the
    // index search still yields the right rows but the pruning is suppressed.
    createPartitionedType();
    populate();

    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = 'acme'");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
    final GetValueFromIndexEntryStep extract = findIndexExtract(plan);
    assertThat(extract).as("index path must include a GetValueFromIndexEntryStep").isNotNull();
    assertThat(extract.getFilterBucketIds())
        .as("partition-pruned index path must constrain to one bucket")
        .hasSize(1);
    rs.close();
  }

  @Test
  void parameterizedQueryDoesNotNarrowIndexFilterBuckets() {
    // Parameter-bound predicates must not bake the bucket id into the cached plan; the
    // GetValueFromIndexEntryStep should see every bucket.
    createPartitionedType();
    populate();

    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = ?", "acme");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
    final GetValueFromIndexEntryStep extract = findIndexExtract(plan);
    assertThat(extract).isNotNull();
    assertThat(extract.getFilterBucketIds())
        .as("parameter-bound queries must NOT prune; every bucket must still be visible")
        .hasSize(BUCKETS);
    rs.close();
  }

  @Test
  void flagSuppressesIndexFilterBucketNarrowing() {
    // With the flag set even literal queries must skip pruning; every bucket id remains visible
    // to the index extract step.
    createPartitionedType();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);
    try {
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = 'acme'");
      final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
      final GetValueFromIndexEntryStep extract = findIndexExtract(plan);
      assertThat(extract).isNotNull();
      assertThat(extract.getFilterBucketIds())
          .as("needsRepartition=true must surface every bucket id at the index extract step")
          .hasSize(BUCKETS);
      rs.close();
    } finally {
      type.setNeedsRepartition(false);
    }
  }

  @Test
  void resultsAreCorrectAcrossEveryTenant() {
    // Sanity check across every partition: each WHERE returns exactly the matching record. Pins
    // that the partition-pruning rule never drops correct rows even when only one bucket is
    // scanned. Goes through the index path in all cases (which ignores filterClusters today),
    // so this asserts the rule is correctness-preserving regardless of which downstream step
    // ultimately consumes the narrowed cluster set.
    createPartitionedType();
    populate();
    for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
      // Use ? parameter binding: pins the contract that pruning is suppressed for parameterised
      // queries. Without that suppression, the planner caches the plan with the first
      // execution's bucket pruning, then later executions with different parameter values get
      // routed to the wrong bucket and silently miss records.
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = ?", tenant);
      assertThat(rs.hasNext()).as("tenant '" + tenant + "' must be findable").isTrue();
      assertThat(rs.next().<String>getProperty("tenant_id")).isEqualTo(tenant);
      assertThat(rs.hasNext()).as("expected exactly one row per tenant").isFalse();
      rs.close();
    }
    // Same loop with literal interpolation: pruning fires here, every record still findable.
    for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = '" + tenant + "'");
      assertThat(rs.hasNext()).as("tenant '" + tenant + "' must be findable via literal").isTrue();
      assertThat(rs.next().<String>getProperty("tenant_id")).isEqualTo(tenant);
      rs.close();
    }
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".payload STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");
    });
  }

  private void populate() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 'acme', payload = 'p-acme'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 'globex', payload = 'p-globex'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 'initech', payload = 'p-initech'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET tenant_id = 'umbrella', payload = 'p-umbrella'");
    });
  }

  private static FetchFromTypeWithFilterStep findFetcher(final ExecutionPlan plan) {
    for (final ExecutionStep step : plan.getSteps())
      if (step instanceof FetchFromTypeWithFilterStep f)
        return f;
    return null;
  }

  private static GetValueFromIndexEntryStep findIndexExtract(final ExecutionPlan plan) {
    for (final ExecutionStep step : plan.getSteps())
      if (step instanceof GetValueFromIndexEntryStep g)
        return g;
    return null;
  }
}
