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

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedVertexType() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(4).create();
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
}
