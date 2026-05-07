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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract that {@code vector.neighbors} on a partitioned type narrows its candidate
 * bucket set to the partition-pruned subset when the query's WHERE clause binds every partition
 * property to a literal (issue #4087, follow-up).
 * <p>
 * Without the planner-to-function plumbing the function would scan every bucket of the type and
 * return the globally-closest vectors regardless of the row's tenant. This test fails on that
 * baseline and passes once the planner stashes the partition-derived bucket file ids on the
 * {@link com.arcadedb.query.sql.executor.CommandContext} and the function intersects them with
 * its own per-type bucket allow-list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionPruningVectorNeighborsTest extends TestHelper {

  private static final String TYPE_NAME = "PartVecDoc";
  private static final int    BUCKETS   = 32;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding ARRAY_OF_FLOATS");
      // Unique on tenant_id is required by PartitionedBucketSelectionStrategy and also serves as
      // the vector index's idPropertyName (so vector.neighbors can resolve a tenant_id to a RID).
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");
      database.command("sql", """
          CREATE INDEX ON %s (embedding) LSM_VECTOR
          METADATA {
            dimensions: 3,
            similarity: 'COSINE',
            idPropertyName: 'tenant_id'
          }""".formatted(TYPE_NAME));
    });

    // The partition strategy enforces UNIQUE on the partition property, so each tenant gets one
    // record. We deliberately pick three tenant names whose {@code hash(name) % 32} land in
    // distinct buckets ({@code acme} -> b27, {@code globex} -> b12, {@code umbrella} -> b5 in
    // the current strategy implementation). {@code initech} would collide with {@code acme} on
    // bucket 27 and is left out so the prune cleanly isolates acme.
    database.transaction(() -> {
      // tenant=acme is FAR from query [1,0,0]: Y-axis vector. Must be the lone resident of its
      // bucket - if it shared a bucket with a close-cluster tenant, the prune would still let
      // that tenant's record slip through and the test would silently mis-pin its contract.
      insertDoc("acme", new float[] { 0.0f, 1.0f, 0.0f });

      // Two close-cluster tenants in OTHER buckets:
      insertDoc("globex", new float[] { 1.0f, 0.0f, 0.0f });
      insertDoc("umbrella", new float[] { 0.98f, 0.02f, 0.0f });
    });
  }

  @Test
  void fixtureSanityTenantBucketsAreDistinct() {
    // Defensive: if a future change to the partition-strategy hash function shifts assignments
    // and one of our chosen tenants now collides with acme, the {@code vectorNeighbors...} tests
    // below would silently degrade (the prune would still narrow but the bucket would contain
    // both tenants). This sanity check makes that failure mode loud.
    final Map<String, Integer> tenantBucket = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT tenant_id, @rid AS rid FROM " + TYPE_NAME)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final RID rid = (RID) r.getProperty("rid");
        tenantBucket.put(r.getProperty("tenant_id"), rid.getBucketId());
      }
    }
    assertThat(tenantBucket).hasSize(3);
    assertThat(tenantBucket.get("acme"))
        .as("acme must own its bucket exclusively for the prune-isolation tests below to be meaningful")
        .isNotEqualTo(tenantBucket.get("globex"))
        .isNotEqualTo(tenantBucket.get("umbrella"));
  }

  @Test
  void vectorNeighborsHonorsPartitionPrune() {
    // WHERE tenant_id='acme' must restrict vector.neighbors to acme's bucket only. With the
    // prune the function sees acme's single Y-axis vector and returns it alone (k=3 caps the
    // result, but only one record lives in that bucket). Without the prune the global top-3
    // are globex/initech/umbrella - so this assertion catches the un-pruned regression.
    // The predicate is a literal (not a parameter) because the planner-side prune intentionally
    // refuses to fire on parameter-bound values - the cached plan would freeze the bucket id at
    // the first execution's binding (see PartitionPruningPlannerTest.parameterizedQuery...).
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.neighbors`('" + TYPE_NAME + "[embedding]', [1.0, 0.0, 0.0], 3) AS neighbors "
            + "FROM " + TYPE_NAME + " WHERE tenant_id = 'acme' LIMIT 1")) {

      assertThat(rs.hasNext()).as("Query must return a row for the acme tenant").isTrue();
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> neighbors = row.getProperty("neighbors");
      assertThat(neighbors).as("neighbors projection must not be null").isNotNull();
      assertThat(neighbors).as("expected at least one partition-restricted neighbour").isNotEmpty();

      // Every returned record must belong to tenant='acme'. Without the prune the closest three
      // globally are globex, initech, umbrella (all NOT acme) and this assertion fails.
      for (final Map<String, Object> neighbour : neighbors) {
        final String tenant = ((Document) neighbour.get("record")).getString("tenant_id");
        assertThat(tenant)
            .as("partition-pruned vector.neighbors must only return acme records, got tenant_id='%s'", tenant)
            .isEqualTo("acme");
      }
    }
  }

  @Test
  void vectorNeighborsWithoutPartitionPredicateScansAllBuckets() {
    // Counter-test: a query without a WHERE on the partition key must NOT trigger pruning, so
    // the function returns the globally-closest neighbours (globex, initech, umbrella). Pins
    // the "no-pruning regression" half of the contract so a future overzealous implementation
    // that always narrows would break this assertion.
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.neighbors`('" + TYPE_NAME + "[embedding]', [1.0, 0.0, 0.0], 3) AS neighbors")) {

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> neighbors = row.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      assertThat(neighbors).hasSizeGreaterThanOrEqualTo(1);

      // At least one of the close-cluster tenants must surface in the global top-3. The fixture
      // only inserts {@code globex} and {@code umbrella} as close-cluster members; {@code initech}
      // is intentionally excluded because it would collide with {@code acme}'s bucket.
      boolean foundCloseCluster = false;
      for (final Map<String, Object> neighbour : neighbors) {
        final String tenant = ((Document) neighbour.get("record")).getString("tenant_id");
        if ("globex".equals(tenant) || "umbrella".equals(tenant)) {
          foundCloseCluster = true;
          break;
        }
      }
      assertThat(foundCloseCluster)
          .as("baseline (no partition prune) must surface at least one of the close-cluster tenants")
          .isTrue();
    }
  }

  private void insertDoc(final String tenant, final float[] embedding) {
    database.newDocument(TYPE_NAME).set("tenant_id", tenant).set("embedding", embedding).save();
  }
}
