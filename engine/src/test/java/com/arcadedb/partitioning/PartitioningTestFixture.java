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
package com.arcadedb.partitioning;

import com.arcadedb.database.Database;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared scaffolding for partition-aware tests across modules. Centralises the
 * {@code CREATE TYPE + index + BucketSelectionStrategy partitioned(tenant_id)} setup so a future
 * change to the partition strategy contract (extra required indexes, default bucket counts,
 * property naming) lands in one place rather than four near-identical helpers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PartitioningTestFixture {

  /** Four canonical tenant ids used by the populate helpers. */
  public static final String[] TENANTS = { "acme", "globex", "initech", "umbrella" };

  private PartitioningTestFixture() {
  }

  /**
   * Creates a partitioned document type with a unique index on {@code tenant_id} and an
   * optional {@code payload} property. The unique index is mandatory: the partitioned bucket
   * selection strategy refuses to attach without one.
   */
  public static void createPartitionedDocType(final Database database, final String typeName, final int buckets,
      final boolean includePayload) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".tenant_id STRING");
      if (includePayload)
        database.command("sql", "CREATE PROPERTY " + typeName + ".payload STRING");
      database.command("sql", "CREATE INDEX ON " + typeName + "(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('tenant_id')`");
    });
  }

  /** Vertex variant of {@link #createPartitionedDocType}. Always includes {@code payload}. */
  public static void createPartitionedVertexType(final Database database, final String typeName, final int buckets) {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName(typeName).withTotalBuckets(buckets).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".tenant_id STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".payload STRING");
      database.command("sql", "CREATE INDEX ON " + typeName + "(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('tenant_id')`");
    });
  }

  /** Inserts one document per {@link #TENANTS} entry. {@code payload} is set to {@code "p-<tenant>"}. */
  public static void populateDocs(final Database database, final String typeName, final boolean includePayload) {
    database.transaction(() -> {
      for (final String tenant : TENANTS) {
        final Map<String, Object> params = new HashMap<>();
        params.put("t", tenant);
        if (includePayload) {
          params.put("p", "p-" + tenant);
          database.command("sql", "INSERT INTO " + typeName + " SET tenant_id = :t, payload = :p", params);
        } else
          database.command("sql", "INSERT INTO " + typeName + " SET tenant_id = :t", params);
      }
    });
  }

  /** Vertex variant of {@link #populateDocs}, always with {@code payload}. */
  public static void populateVertices(final Database database, final String typeName) {
    database.transaction(() -> {
      for (final String tenant : TENANTS) {
        final Map<String, Object> params = new HashMap<>();
        params.put("t", tenant);
        params.put("p", "p-" + tenant);
        database.command("sql", "CREATE VERTEX " + typeName + " SET tenant_id = :t, payload = :p", params);
      }
    });
  }
}
