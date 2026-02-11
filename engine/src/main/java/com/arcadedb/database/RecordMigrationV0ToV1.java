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
import com.arcadedb.serializer.BinaryTypes;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Handles record-level migration from v0 to v1 format.
 * Works directly with Binary buffers without deserializing to avoid type-casting issues.
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
        for (final Bucket bucket : type.getBuckets(false)) {
          if (bucketsToMigrate.contains(bucket.getName())) {
            // Check if it's a vertex or edge segment bucket
            final java.util.concurrent.atomic.AtomicBoolean hasVertices = new java.util.concurrent.atomic.AtomicBoolean(false);
            final java.util.concurrent.atomic.AtomicBoolean hasEdgeSegments = new java.util.concurrent.atomic.AtomicBoolean(false);

            // Quick scan to determine bucket content type
            bucket.scan((rid, view) -> {
              if (view.size() < 1)
                return false; // Stop scan
              final byte recordType = view.getByte(0);
              if (recordType == Vertex.RECORD_TYPE)
                hasVertices.set(true);
              else if (recordType == EdgeSegment.RECORD_TYPE)
                hasEdgeSegments.set(true);
              return false; // Stop after first record
            }, null);

            if (hasVertices.get()) {
              migratedVertices.addAndGet(migrateVertexBucket(bucket));
            } else if (hasEdgeSegments.get()) {
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
   * Migrate vertex bucket from v0 to v1 format by directly converting binary data.
   * v0: [TYPE][OUT_RID: 4+8 bytes][IN_RID: 4+8 bytes][PROPERTIES]
   * v1: [TYPE][OUT_MAP_SIZE: varlong][OUT_ENTRIES...][IN_MAP_SIZE: varlong][IN_ENTRIES...][PROPERTIES]
   */
  private int migrateVertexBucket(final Bucket bucket) {
    LogManager.instance().log(this, Level.INFO, "Migrating vertex bucket: %s", bucket.getName());

    final AtomicInteger count = new AtomicInteger(0);
    final java.util.List<com.arcadedb.utility.Pair<RID, Binary>> recordsToUpdate = new java.util.ArrayList<>();

    bucket.scan((rid, view) -> {
      try {
        if (view.size() < 1)
          return true;

        final byte recordType = view.getByte(0);
        if (recordType != Vertex.RECORD_TYPE)
          return true;

        // Read v0 format
        view.position(1);
        final int outBucketId = view.getInt();
        final long outPosition = view.getLong();
        final int inBucketId = view.getInt();
        final long inPosition = view.getLong();

        // Read properties
        final int propertiesStart = view.position();
        final byte[] propertiesData = new byte[view.size() - propertiesStart];
        view.getByteArray(propertiesStart, propertiesData);

        // Create v1 format buffer
        final Binary v1Buffer = new Binary();
        v1Buffer.putByte(recordType);

        // Write outgoing edges map
        if (outBucketId >= 0 && outPosition >= 0) {
          v1Buffer.putNumber(1); // Map size = 1
          v1Buffer.putNumber(0); // Bucket key = 0
          v1Buffer.putNumber(outBucketId);
          v1Buffer.putNumber(outPosition);
        } else {
          v1Buffer.putNumber(0); // Map size = 0
        }

        // Write incoming edges map
        if (inBucketId >= 0 && inPosition >= 0) {
          v1Buffer.putNumber(1);
          v1Buffer.putNumber(0);
          v1Buffer.putNumber(inBucketId);
          v1Buffer.putNumber(inPosition);
        } else {
          v1Buffer.putNumber(0);
        }

        // Append properties
        v1Buffer.putByteArray(propertiesData);

        recordsToUpdate.add(new com.arcadedb.utility.Pair<>(rid, v1Buffer));
        count.incrementAndGet();

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to convert vertex %s: %s", rid, e.getMessage());
      }
      return true;
    }, null);

    // Write converted records
    for (final com.arcadedb.utility.Pair<RID, Binary> pair : recordsToUpdate) {
      try {
        database.updateRecord(new SimpleRecord(database, pair.getFirst(), pair.getSecond()));
      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to update vertex %s: %s", pair.getFirst(), e.getMessage());
      }
    }

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
    final java.util.List<com.arcadedb.utility.Pair<RID, Binary>> recordsToUpdate = new java.util.ArrayList<>();

    bucket.scan((rid, view) -> {
      try {
        if (view.size() < 1)
          return true;

        final byte recordType = view.getByte(0);
        if (recordType != EdgeSegment.RECORD_TYPE)
          return true;

        // Read v0 format
        view.position(1);
        final int usedBytesV0 = view.getInt();

        // Read PREVIOUS_RID
        final RID previousRID = (RID) database.getSerializer().deserializeValue(database, view, BinaryTypes.TYPE_RID, null);
        final int contentStart = view.position();

        // Count edges
        long edgeCount = 0;
        while (view.position() < usedBytesV0) {
          view.getNumber(); // Edge bucket ID
          view.getNumber(); // Edge position
          view.getNumber(); // Vertex bucket ID
          view.getNumber(); // Vertex position
          edgeCount++;
        }

        // Read entries data
        view.position(contentStart);
        final int entriesLength = usedBytesV0 - contentStart;
        final byte[] entriesData = new byte[entriesLength];
        view.getByteArray(contentStart, entriesData);

        // Create v1 format
        final Binary v1Buffer = new Binary();
        v1Buffer.putByte(recordType);
        v1Buffer.putInt((int) edgeCount); // COUNT at offset 1

        // Placeholder for USED_BYTES
        final int usedBytesPosition = v1Buffer.position();
        v1Buffer.putInt(0);

        // Write PREVIOUS_RID
        database.getSerializer().serializeValue(database, v1Buffer, BinaryTypes.TYPE_RID, previousRID);

        // Append entries
        v1Buffer.putByteArray(entriesData);

        // Update USED_BYTES
        final int finalUsedBytes = v1Buffer.position();
        v1Buffer.putInt(usedBytesPosition, finalUsedBytes);

        recordsToUpdate.add(new com.arcadedb.utility.Pair<>(rid, v1Buffer));
        count.incrementAndGet();

      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to convert edge segment %s: %s", rid, e.getMessage());
      }
      return true;
    }, null);

    // Write converted records
    for (final com.arcadedb.utility.Pair<RID, Binary> pair : recordsToUpdate) {
      try {
        database.updateRecord(new SimpleRecord(database, pair.getFirst(), pair.getSecond()));
      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to update edge segment %s: %s", pair.getFirst(), e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Migrated %d edge segments in bucket %s", count.get(), bucket.getName());
    return count.get();
  }

  /**
   * Simple record wrapper that holds raw binary data for direct bucket updates.
   */
  private static class SimpleRecord implements Record, RecordInternal {
    private final DatabaseInternal database;
    private RID rid;
    private final Binary buffer;

    SimpleRecord(final DatabaseInternal database, final RID rid, final Binary buffer) {
      this.database = database;
      this.rid = rid;
      this.buffer = buffer;
    }

    @Override
    public RID getIdentity() { return rid; }

    @Override
    public void setIdentity(final RID rid) { this.rid = rid; }

    @Override
    public DatabaseInternal getDatabase() { return database; }

    @Override
    public Binary getBuffer() { return buffer; }

    @Override
    public void setBuffer(final Binary buffer) {}

    @Override
    public byte getRecordType() { return buffer.getByte(0); }

    @Override
    public void unsetDirty() {}

    @Override
    public void reload() {
      throw new UnsupportedOperationException("SimpleRecord does not support reload");
    }

    @Override
    public void delete() {
      throw new UnsupportedOperationException("SimpleRecord does not support delete");
    }

    @Override
    public com.arcadedb.serializer.json.JSONObject toJSON(final boolean includeMetadata) {
      throw new UnsupportedOperationException("SimpleRecord does not support JSON");
    }

    @Override
    public int size() {
      return buffer.size();
    }

    @Override
    public Record getRecord(final boolean loadContent) {
      return this;
    }

    @Override
    public Record getRecord() {
      return this;
    }

    @Override
    public com.arcadedb.database.Document asDocument() {
      throw new UnsupportedOperationException("SimpleRecord does not support asDocument");
    }

    @Override
    public com.arcadedb.database.Document asDocument(final boolean loadContent) {
      throw new UnsupportedOperationException("SimpleRecord does not support asDocument");
    }

    @Override
    public com.arcadedb.graph.Vertex asVertex() {
      throw new UnsupportedOperationException("SimpleRecord does not support asVertex");
    }

    @Override
    public com.arcadedb.graph.Vertex asVertex(final boolean loadContent) {
      throw new UnsupportedOperationException("SimpleRecord does not support asVertex");
    }

    @Override
    public com.arcadedb.graph.Edge asEdge() {
      throw new UnsupportedOperationException("SimpleRecord does not support asEdge");
    }

    @Override
    public com.arcadedb.graph.Edge asEdge(final boolean loadContent) {
      throw new UnsupportedOperationException("SimpleRecord does not support asEdge");
    }
  }
}
