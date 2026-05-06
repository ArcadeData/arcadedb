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
import com.arcadedb.partitioning.PartitioningTestFixture;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the partition-aware bucket-pruning rule in {@code MatchNodeStep} for OpenCypher. When the
 * vertex type uses a partitioned bucket strategy and a node pattern's inline properties bind
 * every partition property, {@code MATCH (n:Type {prop: 'X'})} restricts the full-scan fallback
 * to the hash-target bucket. When the type's {@code needsRepartition} flag is set, the rule is
 * suppressed. Issue #4087.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionPruningCypherTest extends TestHelper {

  private static final String TYPE_NAME = "PartV";

  @Test
  void cypherMatchOnPartitionKeyReturnsAllMatches() {
    createPartitionedVertexType();
    populate();
    for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
      final ResultSet rs = database.query("cypher",
          "MATCH (n:" + TYPE_NAME + " {tenant_id: '" + tenant + "'}) RETURN n.tenant_id AS t, n.payload AS p");
      assertThat(rs.hasNext()).as("tenant '" + tenant + "' must be findable via Cypher").isTrue();
      final var row = rs.next();
      assertThat(row.<String>getProperty("t")).isEqualTo(tenant);
      assertThat(row.<String>getProperty("p")).isEqualTo("p-" + tenant);
      rs.close();
    }
  }

  @Test
  void cypherMatchUnderNeedsRepartitionStaysCorrect() {
    // With the flag true, partition pruning is suppressed; queries fall back to the standard
    // unindexed/indexed paths and return correct results. Pins the safety contract.
    createPartitionedVertexType();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);
    try {
      for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
        final ResultSet rs = database.query("cypher",
            "MATCH (n:" + TYPE_NAME + " {tenant_id: '" + tenant + "'}) RETURN n.tenant_id AS t");
        assertThat(rs.hasNext()).as("tenant '" + tenant + "' must remain findable when pruning is suppressed").isTrue();
        assertThat(rs.next().<String>getProperty("t")).isEqualTo(tenant);
        rs.close();
      }
    } finally {
      type.setNeedsRepartition(false);
    }
  }

  @Test
  void cypherMatchOnNonPartitionPropertyStillWorks() {
    // No partition property bound -> pruning skipped, query runs through standard path.
    createPartitionedVertexType();
    populate();
    final ResultSet rs = database.query("cypher",
        "MATCH (n:" + TYPE_NAME + " {payload: 'p-acme'}) RETURN n.tenant_id AS t");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("t")).isEqualTo("acme");
    rs.close();
  }

  @Test
  void cypherMatchOnPartitionLiteralFiresPruning() {
    // tryPartitionPrunedIterator only runs when the leading tryFindAndUseIndex path returns null,
    // so we need a setup where the partition strategy is intact but no index covers the pattern.
    // The strategy mandates a unique index at assignment time but does NOT re-check on every
    // query, so dropping the index after setup is the documented escape hatch for forcing the
    // bucket-iteration fallback. Without this exact assertion, a refactor that silently breaks
    // tryPartitionPrunedIterator would still pass every correctness test (the type-scan
    // fallback would just iterate every bucket).
    createPartitionedVertexTypeThenDropIndex();
    populate();

    final String plan = profilePlan("MATCH (n:" + TYPE_NAME + " {tenant_id: 'acme'}) RETURN n.tenant_id AS t");
    assertThat(plan)
        .as("MATCH NODE prettyPrint must mark the partition-pruned bucket so a regression in "
            + "tryPartitionPrunedIterator becomes visible")
        .contains("[partition:");
  }

  @Test
  void cypherMatchUnderNeedsRepartitionDoesNotFirePruning() {
    // With the flag set, tryPartitionPrunedIterator must bail before binding usedPartitionBucket.
    // Pins the suppression contract from the planner side, complementing the data-correctness
    // test above. Same drop-index setup so we exercise the bucket-iteration fallback.
    createPartitionedVertexTypeThenDropIndex();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);
    try {
      final String plan = profilePlan("MATCH (n:" + TYPE_NAME + " {tenant_id: 'acme'}) RETURN n.tenant_id AS t");
      assertThat(plan)
          .as("needsRepartition=true must suppress the partition-pruning marker")
          .doesNotContain("[partition:");
    } finally {
      type.setNeedsRepartition(false);
    }
  }

  @Test
  void cypherMatchWithParameterDoesNotFirePruning() {
    // Parameter-bound partition values would bake the bucket id into a cached plan and misroute
    // later executions; tryPartitionPrunedIterator must reject them. Pins the parameter-skip
    // contract on the Cypher side.
    createPartitionedVertexTypeThenDropIndex();
    populate();

    final java.util.Map<String, Object> params = new java.util.HashMap<>();
    params.put("t", "acme");
    final ResultSet rs = database.query("cypher",
        "PROFILE MATCH (n:" + TYPE_NAME + " {tenant_id: $t}) RETURN n.tenant_id AS x", params);
    while (rs.hasNext())
      rs.next();
    final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    rs.close();
    assertThat(plan)
        .as("parameter-bound partition value must not trigger the pruning marker")
        .doesNotContain("[partition:");
  }

  private String profilePlan(final String cypher) {
    // PROFILE forces the engine to surface the executed plan via getExecutionPlan(), which is
    // empty for plain query() calls.
    final ResultSet rs = database.query("cypher", "PROFILE " + cypher);
    while (rs.hasNext())
      rs.next();
    final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    rs.close();
    return plan;
  }

  private void createPartitionedVertexTypeThenDropIndex() {
    // Mirror createPartitionedVertexType, then drop the unique index so MatchNodeStep's
    // index path returns null and the partition-pruning fallback can fire. The strategy's
    // setType invariant (`requires a unique index on partition properties`) is checked only at
    // assignment, so the drop is allowed afterwards. PartitionedBucketSelectionStrategy keeps
    // routing via its own hash so inserts continue to land in the right bucket.
    createPartitionedVertexType();
    database.transaction(() -> {
      for (final var idx : database.getSchema().getType(TYPE_NAME).getAllIndexes(false))
        database.getSchema().dropIndex(idx.getName());
    });
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedVertexType() {
    PartitioningTestFixture.createPartitionedVertexType(database, TYPE_NAME, 4);
  }

  private void populate() {
    PartitioningTestFixture.populateVertices(database, TYPE_NAME);
  }
}
