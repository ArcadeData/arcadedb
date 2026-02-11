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

  public RecordMigrationV0ToV1(final DatabaseInternal database) {
    this.database = database;
  }

  /**
   * Migrate all records from v0 to v1 format.
   */
  public void migrate() {
    LogManager.instance().log(this, Level.INFO, "Starting record-level migration from v0 to v1...");

    final long startTime = System.currentTimeMillis();
    final AtomicInteger migratedVertices = new AtomicInteger(0);
    final AtomicInteger migratedEdgeSegments = new AtomicInteger(0);

    database.begin();
    try {
      // Migrate vertex records
      for (final DocumentType type : database.getSchema().getTypes()) {
        if (type instanceof LocalVertexType) {
          for (final Bucket bucket : type.getBuckets(false)) {
            migratedVertices.addAndGet(migrateVertexBucket(bucket));
          }
        }
      }

      // Migrate edge segment records
      for (final DocumentType type : database.getSchema().getTypes()) {
        for (final Bucket bucket : type.getBuckets(false)) {
          if (bucket.getName().contains("edge-segment")) {
            migratedEdgeSegments.addAndGet(migrateEdgeSegmentBucket(bucket));
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
   * v0: [TYPE][OUT_RID: 4+8 bytes][IN_RID: 4+8 bytes][PROPERTIES]
   * v1: [TYPE][OUT_MAP_SIZE: varlong][OUT_ENTRIES...][IN_MAP_SIZE: varlong][IN_ENTRIES...][PROPERTIES]
   */
  private int migrateVertexBucket(final Bucket bucket) {
    LogManager.instance().log(this, Level.INFO, "Migrating vertex bucket: %s", bucket.getName());

    final AtomicInteger count = new AtomicInteger(0);

    // Collect records to migrate (can't modify during scan)
    bucket.scan((rid, view) -> {
      try {
        if (view.size() < 1)
          return true;

        final byte recordType = view.getByte(0);
        if (recordType != Vertex.RECORD_TYPE)
          return true;

        // Read v0 format (fixed-size RIDs)
        view.position(1); // Skip TYPE
        final int outBucketId = view.getInt();
        final long outPosition = view.getLong();
        final int inBucketId = view.getInt();
        final long inPosition = view.getLong();

        // Read properties (everything after the edge RIDs)
        final int propertiesStart = view.position();
        final byte[] propertiesData = new byte[view.size() - propertiesStart];
        view.getByteArray(propertiesStart, propertiesData);

        // Create v1 format buffer with varlong encoding
        final Binary v1Buffer = new Binary();
        v1Buffer.putByte(recordType);

        // Write outgoing edges map
        if (outBucketId >= 0 && outPosition >= 0) {
          v1Buffer.putNumber(1); // Map size = 1
          v1Buffer.putNumber(0); // Bucket key = 0 (v0 used single list)
          v1Buffer.putNumber(outBucketId); // RID bucket ID
          v1Buffer.putNumber(outPosition); // RID position
        } else {
          v1Buffer.putNumber(0); // Map size = 0 (no outgoing edges)
        }

        // Write incoming edges map
        if (inBucketId >= 0 && inPosition >= 0) {
          v1Buffer.putNumber(1); // Map size = 1
          v1Buffer.putNumber(0); // Bucket key = 0
          v1Buffer.putNumber(inBucketId); // RID bucket ID
          v1Buffer.putNumber(inPosition); // RID position
        } else {
          v1Buffer.putNumber(0); // Map size = 0 (no incoming edges)
        }

        // Append properties
        v1Buffer.putByteArray(propertiesData);

        // Create a temporary mutable document to update the record
        final DocumentType type = database.getSchema().getTypeByBucketId(rid.getBucketId());
        final MutableDocument doc = new MutableDocument(database, type, rid, v1Buffer);
        doc.save();

        count.incrementAndGet();

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to migrate vertex %s: %s", rid, e.getMessage());
      }
      return true;
    }, null);

    LogManager.instance().log(this, Level.INFO, "Migrated %d vertices in bucket %s", count.get(), bucket.getName());
    return count.get();
  }

  /**
   * Migrate edge segment bucket from v0 to v1 format.
   * v0: [TYPE][USED_BYTES][PREVIOUS_RID][ENTRIES...]
   * v1: [TYPE][COUNT][USED_BYTES][PREVIOUS_RID][ENTRIES...]
   */
  private int migrateEdgeSegmentBucket(final Bucket bucket) {
    LogManager.instance().log(this, Level.INFO, "Migrating edge segment bucket: %s", bucket.getName());

    final AtomicInteger count = new AtomicInteger(0);

    bucket.scan((rid, view) -> {
      try {
        if (view.size() < 1)
          return true;

        final byte recordType = view.getByte(0);
        if (recordType != EdgeSegment.RECORD_TYPE)
          return true;

        // Read v0 format: [TYPE][USED_BYTES][PREVIOUS_RID][ENTRIES...]
        view.position(1);
        final int usedBytesV0 = view.getInt(); // Offset 1 in v0

        // Read PREVIOUS_RID
        final RID previousRID = (RID) database.getSerializer().deserializeValue(database, view, BinaryTypes.TYPE_RID, null);

        final int contentStart = view.position();

        // Count edges by scanning entries
        long edgeCount = 0;
        while (view.position() < usedBytesV0) {
          view.getNumber(); // Edge bucket ID
          view.getNumber(); // Edge position
          view.getNumber(); // Vertex bucket ID
          view.getNumber(); // Vertex position
          edgeCount++;
        }

        // Read all entries data
        view.position(contentStart);
        final int entriesLength = usedBytesV0 - contentStart;
        final byte[] entriesData = new byte[entriesLength];
        view.getByteArray(contentStart, entriesData);

        // Create v1 format: [TYPE][COUNT][USED_BYTES][PREVIOUS_RID][ENTRIES...]
        final Binary v1Buffer = new Binary();
        v1Buffer.putByte(recordType); // Offset 0
        v1Buffer.putInt((int) edgeCount); // Offset 1 - COUNT at fixed position

        // Placeholder for USED_BYTES (we'll update it after writing PREVIOUS_RID)
        final int usedBytesPosition = v1Buffer.position();
        v1Buffer.putInt(0); // Offset 5

        // Write PREVIOUS_RID (offset 9)
        database.getSerializer().serializeValue(database, v1Buffer, BinaryTypes.TYPE_RID, previousRID);

        final int v1ContentStart = v1Buffer.position();

        // Append entries
        v1Buffer.putByteArray(entriesData);

        // Update USED_BYTES
        final int finalUsedBytes = v1Buffer.position();
        v1Buffer.putInt(usedBytesPosition, finalUsedBytes);

        // Update record using MutableDocument
        final DocumentType type = database.getSchema().getTypeByBucketId(rid.getBucketId());
        final MutableDocument doc = new MutableDocument(database, type, rid, v1Buffer);
        doc.save();

        count.incrementAndGet();

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to migrate edge segment %s: %s", rid, e.getMessage());
      }
      return true;
    }, null);

    LogManager.instance().log(this, Level.INFO, "Migrated %d edge segments in bucket %s", count.get(), bucket.getName());
    return count.get();
  }
}
