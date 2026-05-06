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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.partitioning.PartitioningTestFixture;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins {@code REBUILD TYPE <Type> WITH repartition = true}. Verifies that running the rebuild
 * (a) clears the type's {@code needsRepartition} flag, (b) reports the moved-record count, and
 * (c) leaves all records still findable. Issue #4087.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildTypeRepartitionTest extends TestHelper {

  private static final String TYPE_NAME = "PartDoc";

  @Test
  void rebuildTypeWithRepartitionClearsTheFlag() {
    createPartitionedType();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);
    assertThat(type.isNeedsRepartition()).isTrue();

    final ResultSet rs = database.command("sql", "REBUILD TYPE " + TYPE_NAME + " WITH repartition = true");
    final Result row = rs.next();
    rs.close();

    assertThat(row.<Boolean>getProperty("repartition")).isTrue();
    assertThat(row.<Long>getProperty("recordsRebuilt")).isEqualTo(4L);
    assertThat(row.<Long>getProperty("recordsMoved")).isNotNull();
    assertThat(type.isNeedsRepartition())
        .as("a successful repartition rebuild must clear the needsRepartition flag")
        .isFalse();
  }

  @Test
  void recordsRemainFindableAfterRepartitionRebuild() {
    createPartitionedType();
    populate();

    // Force a stale-mapping state by adding a bucket (PartitionedBucketSelectionStrategy
    // semantically invalidates the modulus); the rebuild must move the affected records back
    // into their hash-target buckets and clear the flag.
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    final LocalBucket extra = (LocalBucket) database.getSchema().createBucket(TYPE_NAME + "_extra");
    type.addBucket(extra);
    assertThat(type.isNeedsRepartition()).isTrue();

    database.command("sql", "REBUILD TYPE " + TYPE_NAME + " WITH repartition = true").close();

    assertThat(type.isNeedsRepartition()).isFalse();

    // Every tenant must still be findable after the rebuild. Records may have new RIDs.
    for (final String tenant : new String[] { "acme", "globex", "initech", "umbrella" }) {
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE tenant_id = '" + tenant + "'");
      assertThat(rs.hasNext()).as("tenant '" + tenant + "' must remain findable after repartition").isTrue();
      assertThat(rs.next().<String>getProperty("tenant_id")).isEqualTo(tenant);
      rs.close();
    }
  }

  @Test
  void rebuildWithoutRepartitionLeavesFlagAlone() {
    createPartitionedType();
    populate();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);

    database.command("sql", "REBUILD TYPE " + TYPE_NAME).close();

    assertThat(type.isNeedsRepartition())
        .as("REBUILD TYPE without `repartition = true` must NOT touch the flag")
        .isTrue();
    type.setNeedsRepartition(false); // reset for the next test
  }

  @Test
  void unknownSettingIsRejected() {
    createPartitionedType();
    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE " + TYPE_NAME + " WITH foo = 'bar'"))
        .hasMessageContaining("Unrecognized setting")
        .hasMessageContaining("repartition");
  }

  @Test
  void repartitionMustBeBoolean() {
    createPartitionedType();
    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE " + TYPE_NAME + " WITH repartition = 42"))
        .hasMessageContaining("repartition")
        .hasMessageContaining("true or false");
  }

  @Test
  void repartitionOnVertexTypeIsRefused() {
    // Vertex repartition would give every vertex a new RID. Inbound edges from other vertices
    // would silently dangle, and outbound-edge segments stored on the vertex are wiped by the
    // delete+re-insert. There's no safe in-place fix, so the rebuild is refused outright; the
    // documented workflow is drop and re-import under the new partitioning.
    createPartitionedVertexType();
    populateVertices();

    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE PartV WITH repartition = true"))
        .hasMessageContaining("vertex")
        .hasMessageContaining("repartition")
        .hasMessageContaining("not supported");
  }

  @Test
  void repartitionOnEdgeTypeIsRefused() {
    // Edge RIDs are referenced from adjacent vertices' edge segments; repartitioning an edge
    // type would orphan every such reference. Same unconditional refusal as vertex types.
    database.transaction(() -> {
      database.getSchema().buildEdgeType().withName("PartE").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY PartE.kind STRING");
      database.command("sql", "CREATE INDEX ON PartE(kind) UNIQUE");
      database.command("sql", "ALTER TYPE PartE BucketSelectionStrategy `partitioned('kind')`");
    });

    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE PartE WITH repartition = true"))
        .hasMessageContaining("edge")
        .hasMessageContaining("repartition")
        .hasMessageContaining("not supported");
  }

  @Test
  void alterTypeRepartitionOnVertexIsRefused() {
    // The atomic ALTER TYPE WITH repartition path chains a REBUILD; the guard must trip there
    // too so the destructive path is consistent regardless of entry point.
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("PartV").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY PartV.tenant_id STRING");
      database.command("sql", "CREATE PROPERTY PartV.payload STRING");
      database.command("sql", "CREATE INDEX ON PartV(tenant_id) UNIQUE");
    });
    assertThatThrownBy(() ->
        database.command("sql",
            "ALTER TYPE PartV BucketSelectionStrategy `partitioned('tenant_id')` WITH repartition = true"))
        .hasMessageContaining("vertex")
        .hasMessageContaining("not supported");
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    PartitioningTestFixture.createPartitionedDocType(database, TYPE_NAME, 4, true);
  }

  private void populate() {
    PartitioningTestFixture.populateDocs(database, TYPE_NAME, true);
  }

  private void createPartitionedVertexType() {
    PartitioningTestFixture.createPartitionedVertexType(database, "PartV", 4);
  }

  private void populateVertices() {
    PartitioningTestFixture.populateVertices(database, "PartV");
  }
}
