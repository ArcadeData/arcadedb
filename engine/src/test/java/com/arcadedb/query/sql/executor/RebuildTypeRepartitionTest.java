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
  void repartitionOnVertexTypeRefusesWithoutForce() {
    // Vertex repartition gives every vertex a new RID and silently breaks every edge that
    // references it. The guard must surface that destruction up front so an operator can stop or
    // explicitly opt in.
    createPartitionedVertexType();
    populateVertices();

    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE PartV WITH repartition = true"))
        .hasMessageContaining("vertex")
        .hasMessageContaining("repartition")
        .hasMessageContaining("force = true");
  }

  @Test
  void repartitionOnVertexTypeWithForceProceeds() {
    // Force escape hatch for users who have no edges (or have already re-pointed them). The
    // command must run to completion and clear the needsRepartition flag.
    createPartitionedVertexType();
    populateVertices();

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("PartV");
    type.setNeedsRepartition(true);
    final ResultSet rs = database.command("sql", "REBUILD TYPE PartV WITH repartition = true, force = true");
    final Result row = rs.next();
    rs.close();
    assertThat(row.<Boolean>getProperty("repartition")).isTrue();
    assertThat(type.isNeedsRepartition()).isFalse();
  }

  @Test
  void repartitionOnEdgeTypeRefusesWithoutForce() {
    // Edge RIDs are stored on adjacent vertices' edge segments; repartitioning an edge type
    // would orphan every reference. Same guard as vertex types.
    database.transaction(() -> {
      database.getSchema().buildEdgeType().withName("PartE").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY PartE.kind STRING");
      database.command("sql", "CREATE INDEX ON PartE(kind) UNIQUE");
      database.command("sql", "ALTER TYPE PartE BucketSelectionStrategy `partitioned('kind')`");
    });

    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE PartE WITH repartition = true"))
        .hasMessageContaining("edge")
        .hasMessageContaining("repartition")
        .hasMessageContaining("force = true");
  }

  @Test
  void alterTypeRepartitionOnVertexRefusesWithoutForce() {
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
        .hasMessageContaining("force = true");
  }

  @Test
  void alterTypeRepartitionOnVertexWithForceProceeds() {
    // Force is plumbed through the ALTER TYPE → REBUILD chain.
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("PartV").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY PartV.tenant_id STRING");
      database.command("sql", "CREATE PROPERTY PartV.payload STRING");
      database.command("sql", "CREATE INDEX ON PartV(tenant_id) UNIQUE");
    });
    final ResultSet rs = database.command("sql",
        "ALTER TYPE PartV BucketSelectionStrategy `partitioned('tenant_id')` WITH repartition = true, force = true");
    final Result row = rs.next();
    rs.close();
    assertThat(row.<String>getProperty("operation")).isEqualTo("ALTER TYPE");
    assertThat(row.<String>getProperty("result")).isEqualTo("OK");
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(4).create();
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

  private void createPartitionedVertexType() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("PartV").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY PartV.tenant_id STRING");
      database.command("sql", "CREATE PROPERTY PartV.payload STRING");
      database.command("sql", "CREATE INDEX ON PartV(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE PartV BucketSelectionStrategy `partitioned('tenant_id')`");
    });
  }

  private void populateVertices() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX PartV SET tenant_id = 'acme', payload = 'p-acme'");
      database.command("sql", "CREATE VERTEX PartV SET tenant_id = 'globex', payload = 'p-globex'");
      database.command("sql", "CREATE VERTEX PartV SET tenant_id = 'initech', payload = 'p-initech'");
      database.command("sql", "CREATE VERTEX PartV SET tenant_id = 'umbrella', payload = 'p-umbrella'");
    });
  }
}
