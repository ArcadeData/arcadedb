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
package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.serializer.BinaryTypes;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Handles record-level migration from v0 to v1 format.
 * Works directly with Binary buffers to avoid deserialization issues.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RecordMigrationV0ToV1 {
  private final DatabaseInternal database;
  private final java.util.Set<String> bucketsToMigrate;

  public RecordMigrationV0ToV1(final DatabaseInternal database, final java.util.Set<String> bucketsToMigrate) {
    this.database = database;
    this.bucketsToMigrate = bucketsToMigrate;
  }

  /**
   * Migrate records from v0 to v1 format for the specified buckets only.
   */
  public void migrate() {

    if (bucketsToMigrate == null || bucketsToMigrate.isEmpty()) {
      LogManager.instance().log(this, Level.INFO, "No buckets to migrate (list is empty)");
      return;
    }

    LogManager.instance().log(this, Level.INFO,
        "Starting record-level migration from v0 to v1 for %d buckets...", bucketsToMigrate.size());

    final long startTime = System.currentTimeMillis();
    final AtomicInteger migratedVertices = new AtomicInteger(0);
    final AtomicInteger migratedEdgeSegments = new AtomicInteger(0);

    database.begin();
    try {
      // Migrate vertex records (only for buckets in the migration list)
      for (final DocumentType type : database.getSchema().getTypes()) {
        if (type instanceof LocalVertexType) {
          for (final Bucket bucket : type.getBuckets(false)) {
            if (bucketsToMigrate.contains(bucket.getName())) {
              migratedVertices.addAndGet(migrateVertexBucket(bucket));
            }
          }
        }
      }

      // Migrate edge segment records (only for buckets in the migration list)
      for (final DocumentType type : database.getSchema().getTypes()) {
        for (final Bucket bucket : type.getBuckets(false)) {
          if (bucketsToMigrate.contains(bucket.getName())) {
            if (bucket.getName().contains("edge-segment")) {
              migratedEdgeSegments.addAndGet(migrateEdgeSegmentBucket(bucket));
            }
          }
        }
      }

      database.commit();

      final long elapsed = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO,
          "Record migration completed: %d vertices, %d edge segments in %d ms",
          migratedVertices.get(), migratedEdgeSegments.get(), elapsed);

    } catch (Exception e) {
      database.rollback();
      LogManager.instance().log(this, Level.SEVERE, "Record migration failed", e);
      throw new DatabaseOperationException("Record migration from v0 to v1 failed", e);
    }
  }

  /**
   * Migrate vertex bucket from v0 to v1 format.
   * Strategy: Load v0 vertices (backward-compatible reading), modify them (triggers v1 write).
   * v0: [TYPE][OUT_RID: 4+8 bytes][IN_RID: 4+8 bytes][PROPERTIES]
   * v1: [TYPE][OUT_MAP_SIZE: varlong][OUT_ENTRIES...][IN_MAP_SIZE: varlong][IN_ENTRIES...][PROPERTIES]
   */
  private int migrateVertexBucket(final Bucket bucket) {
    LogManager.instance().log(this, Level.INFO, "Migrating vertex bucket: %s", bucket.getName());

    final AtomicInteger count = new AtomicInteger(0);

    // Collect RIDs to migrate (can't modify during scan)
    final java.util.List<RID> ridsToMigrate = new java.util.ArrayList<>();

    bucket.scan((rid, view) -> {
      try {
        if (view.size() < 1)
          return true;

        final byte recordType = view.getByte(0);
        if (recordType != Vertex.RECORD_TYPE)
          return true;

        ridsToMigrate.add(rid);

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to scan vertex %s: %s", rid, e.getMessage());
      }
      return true;
    }, null);

    // Now load each vertex and save it (which will convert to v1 format)
    for (final RID rid : ridsToMigrate) {
      try {
        // Load vertex using backward-compatible reading (reads v0, deserializes to in-memory vertex)
        final com.arcadedb.database.Record record = database.lookupByRID(rid, false);

        // Convert to mutable vertex
        final com.arcadedb.graph.MutableVertex vertex = record.asVertex().modify();

        // Save it back (will serialize in v1 format)
        vertex.save();

        count.incrementAndGet();

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to migrate vertex %s: %s", rid, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Migrated %d vertices in bucket %s", count.get(), bucket.getName());
    return count.get();
  }

  /**
   * Migrate edge segment bucket from v0 to v1 format.
   * Strategy: Load v0 edge segments (backward-compatible reading), modify and save (triggers v1 write).
   * v0: [TYPE][USED_BYTES][PREVIOUS_RID][ENTRIES...]
   * v1: [TYPE][COUNT][USED_BYTES][PREVIOUS_RID][ENTRIES...]
   */
  private int migrateEdgeSegmentBucket(final Bucket bucket) {
    LogManager.instance().log(this, Level.INFO, "Migrating edge segment bucket: %s", bucket.getName());

    final AtomicInteger count = new AtomicInteger(0);

    // Collect RIDs to migrate
    final java.util.List<RID> ridsToMigrate = new java.util.ArrayList<>();

    bucket.scan((rid, view) -> {
      try {
        if (view.size() < 1)
          return true;

        final byte recordType = view.getByte(0);
        if (recordType != EdgeSegment.RECORD_TYPE)
          return true;

        ridsToMigrate.add(rid);

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to scan edge segment %s: %s", rid, e.getMessage());
      }
      return true;
    }, null);

    // Now load each edge segment and save it (which will convert to v1 format)
    for (final RID rid : ridsToMigrate) {
      try {
        // Load edge segment as mutable (will trigger v0 backward-compatible reading)
        // Then save it back (will serialize in v1 format)
        final com.arcadedb.graph.MutableEdgeSegment mutableSegment =
            new com.arcadedb.graph.MutableEdgeSegment(database, rid);

        // Save it (will serialize in v1 format)
        database.updateRecord(mutableSegment);

        count.incrementAndGet();

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to migrate edge segment %s: %s", rid, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Migrated %d edge segments in bucket %s", count.get(), bucket.getName());
    return count.get();
  }
}
