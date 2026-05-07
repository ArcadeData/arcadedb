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
 * Sparse-vector counterpart of {@link PartitionPruningVectorNeighborsTest}: pins the contract
 * that {@code vector.sparseNeighbors} on a partitioned type narrows its candidate bucket set to
 * the partition-pruned subset when the query's WHERE clause binds every partition property to a
 * literal (issue #4087, follow-up).
 * <p>
 * The dense and sparse neighbour functions take separate code paths to assemble the per-bucket
 * index list ({@code SQLFunctionVectorNeighbors.execute} vs
 * {@code SQLFunctionVectorSparseNeighbors.collectSparseIndexes}). The narrowing helper is shared
 * but each call site invokes it independently, so a regression in the sparse path would not
 * surface in the dense test - hence the dedicated coverage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionPruningVectorSparseNeighborsTest extends TestHelper {

  private static final String TYPE_NAME = "PartSparseDoc";
  private static final int    BUCKETS   = 32;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });

    // Three tenants in distinct buckets (acme -> b27, globex -> b12, umbrella -> b5 under the
    // current partition strategy hash; verified by {@link #fixtureSanityTenantBucketsAreDistinct}).
    // Sparse vectors all touch dimension 1 so the dot-product query can score every record - what
    // distinguishes acme is its tiny weight, which keeps it out of the global top-3 ranking but
    // makes it the lone resident of its bucket once the partition prune isolates the search.
    database.transaction(() -> {
      // acme: small weight on shared dim 1 -> low global rank.
      insertDoc("acme", new int[] { 1 }, new float[] { 0.05f });

      // Close-cluster tenants: high weights on shared dim 1.
      insertDoc("globex", new int[] { 1 }, new float[] { 1.0f });
      insertDoc("umbrella", new int[] { 1 }, new float[] { 0.9f });
    });
  }

  @Test
  void fixtureSanityTenantBucketsAreDistinct() {
    // Same defensive sanity check as the dense test: a future hash-function shift that puts two of
    // our tenants in the same bucket would silently weaken the prune-isolation tests below.
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
  void sparseNeighborsHonorsPartitionPrune() {
    // WHERE tenant_id='acme' must restrict vector.sparseNeighbors to acme's bucket only. With
    // the prune the function sees acme's lone low-score record and returns it; without the prune
    // the global top-3 are globex/umbrella/acme (all three tenants share dim 1) but globex and
    // umbrella outrank acme by weight - the assertion below catches the un-pruned regression by
    // requiring every returned record to be acme's.
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.sparseNeighbors`('" + TYPE_NAME + "[tokens,weights]', [1], [1.0], 3) AS neighbors "
            + "FROM " + TYPE_NAME + " WHERE tenant_id = 'acme' LIMIT 1")) {

      assertThat(rs.hasNext()).as("Query must return a row for the acme tenant").isTrue();
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> neighbors = row.getProperty("neighbors");
      assertThat(neighbors).as("neighbors projection must not be null").isNotNull();
      assertThat(neighbors).as("expected at least one partition-restricted neighbour").isNotEmpty();

      for (final Map<String, Object> neighbour : neighbors) {
        final String tenant = ((Document) neighbour.get("record")).getString("tenant_id");
        assertThat(tenant)
            .as("partition-pruned vector.sparseNeighbors must only return acme records, got tenant_id='%s'", tenant)
            .isEqualTo("acme");
      }
    }
  }

  @Test
  void sparseNeighborsWithoutPartitionPredicateScansAllBuckets() {
    // Counter-test: a query without a WHERE on the partition key must NOT trigger pruning. The
    // global top-3 should include at least one of the close-cluster tenants whose weight on the
    // shared dim outranks acme's.
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.sparseNeighbors`('" + TYPE_NAME + "[tokens,weights]', [1], [1.0], 3) AS neighbors")) {

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> neighbors = row.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      assertThat(neighbors).hasSizeGreaterThanOrEqualTo(1);

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

  private void insertDoc(final String tenant, final int[] tokens, final float[] weights) {
    database.newDocument(TYPE_NAME).set("tenant_id", tenant).set("tokens", tokens).set("weights", weights).save();
  }
}
