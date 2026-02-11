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
package com.arcadedb.graph;

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.concurrent.atomic.AtomicInteger;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * EdgeSegment v1 format with count caching.
 * Format: [RECORD_TYPE: 1][COUNT: 4][USED_BYTES: 4][PREVIOUS_RID: variable][ENTRIES...]
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MutableEdgeSegment extends BaseRecord implements EdgeSegment, RecordInternal {
  public static final byte RECORD_TYPE    = 3;
  public static final byte FORMAT_VERSION = 1; // For reference only, not stored in record

  // v1 format: COUNT at fixed position right after RECORD_TYPE
  private static final int COUNT_POSITION        = Binary.BYTE_SERIALIZED_SIZE;
  private static final int USED_BYTES_POSITION   = COUNT_POSITION + Binary.INT_SERIALIZED_SIZE;
  private static final int PREVIOUS_RID_POSITION = USED_BYTES_POSITION + Binary.INT_SERIALIZED_SIZE;

  private final RID  NULL_RID;
  private       int  bufferSize;
  // Cached edge count (unsigned int, 0 to 4,294,967,295)
  private       long totalCount;

  public MutableEdgeSegment(final Database database, final RID rid) {
    super(database, rid, null);
    NULL_RID = new RID(database, -1, -1);
    this.buffer = null;
    this.totalCount = 0;
  }

  public MutableEdgeSegment(final Database database, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
    NULL_RID = new RID(database, -1, -1);
    this.buffer = buffer;
    if (buffer != null) {
      this.buffer.setAutoResizable(false);
      this.bufferSize = buffer.size();

      // v1 format: [RECORD_TYPE][COUNT][USED_BYTES][PREVIOUS_RID][ENTRIES...]
      // COUNT is at fixed position right after RECORD_TYPE
      this.totalCount = Integer.toUnsignedLong(buffer.getInt(COUNT_POSITION));

      // Position buffer at content start (after variable-length PREVIOUS_RID)
      buffer.position(PREVIOUS_RID_POSITION);
      ((DatabaseInternal) database).getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_RID, null);
      // Buffer is now positioned at content start
    } else {
      this.totalCount = 0;
    }
  }

  @Override
  public EdgeSegment copy() {
    return new MutableEdgeSegment(database, rid, buffer.copyOfContent());
  }

  public MutableEdgeSegment(final DatabaseInternal database, final int bufferSize) {
    super(database, null, new Binary(bufferSize));
    NULL_RID = new RID(database, -1, -1);
    this.buffer.setAutoResizable(false);
    this.bufferSize = bufferSize;
    this.totalCount = 0;

    // v1 format: [RECORD_TYPE][COUNT][USED_BYTES][PREVIOUS_RID][ENTRIES...]
    buffer.putByte(0, RECORD_TYPE);
    buffer.putInt(COUNT_POSITION, (int) totalCount); // COUNT at fixed position
    buffer.position(PREVIOUS_RID_POSITION);
    database.getSerializer().serializeValue(database, buffer, BinaryTypes.TYPE_RID, NULL_RID); // PREVIOUS

    // Content starts here (after variable-length PREVIOUS_RID)
    final int contentStartPosition = buffer.position();
    buffer.putInt(USED_BYTES_POSITION, contentStartPosition); // USED_BYTES
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  /**
   * If there is room in the current segment, adds the RID at the beginning of the list. This allows to maintain the
   * list of edges always ordered
   * by descending insertion time.
   *
   * @param edgeRID
   * @param vertexRID
   * @return
   */
  @Override
  public boolean add(final RID edgeRID, final RID vertexRID) {
    final Binary ridSerialized = database.getContext().getTemporaryBuffer1();
    database.getSerializer().serializeValue(database, ridSerialized, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
    database.getSerializer().serializeValue(database, ridSerialized, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

    final int used = getUsed();
    final int ridSerializedSize = ridSerialized.size();
    final int contentStartPos = getContentStartPosition();

    if (used + ridSerializedSize <= bufferSize) {
      // APPEND AT THE BEGINNING OF THE CURRENT CHUNK
      buffer.move(contentStartPos, contentStartPos + ridSerializedSize, used - contentStartPos);

      buffer.putByteArray(contentStartPos, ridSerialized.getContent(), ridSerialized.getContentBeginOffset(),
          ridSerializedSize);

      // UPDATE USED BYTES
      setUsed(used + ridSerializedSize);

      // INCREMENT COUNT
      totalCount++;
      updateCachedCount();

      return true;
    }

    // NO ROOM
    return false;
  }

  @Override
  public boolean containsEdge(final RID rid) {
    final int used = getUsed();
    if (used == 0)
      return false;

    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();
    final int contentStartPos = getContentStartPosition();

    buffer.position(contentStartPos);

    while (buffer.position() < used) {
      final int currEdgeBucketId = (int) buffer.getNumber();
      final long currEdgePosition = buffer.getNumber();
      if (currEdgeBucketId == bucketId && currEdgePosition == position)
        return true;

      // SKIP VERTEX RID
      buffer.getNumber();
      buffer.getNumber();
    }

    return false;
  }

  @Override
  public RID getFirstEdgeConnectedToVertex(final RID rid, final int[] edgeBucketFilter) {
    final int used = getUsed();
    if (used == 0)
      return null;

    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();
    final int contentStartPos = getContentStartPosition();

    buffer.position(contentStartPos);

    while (buffer.position() < used) {
      final int currEdgeBucketId = (int) buffer.getNumber();
      final long currEdgePositionInBucket = buffer.getNumber();

      final int currVertexBucketId = (int) buffer.getNumber();
      final long currVertexPosition = buffer.getNumber();
      if (currVertexBucketId == bucketId && currVertexPosition == position) {
        if (edgeBucketFilter != null) {
          // FILTER BY EDGE BUCKETS
          for (int i = 0; i < edgeBucketFilter.length; i++) {
            if (currEdgeBucketId == edgeBucketFilter[i])
              return new RID(database, currEdgeBucketId, currEdgePositionInBucket);
          }
        } else
          return new RID(database, currEdgeBucketId, currEdgePositionInBucket);
      }
    }

    return null;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = new JSONObject().put(RID_PROPERTY, getIdentity().toString());
    final int used = getUsed();
    if (used > 0) {
      final JSONArray entries = new JSONArray();
      final int contentStartPos = getContentStartPosition();

      buffer.position(contentStartPos);
      while (buffer.position() < used) {
        new JSONObject()
            // EDGE RID
            .put("edge", "#" + buffer.getNumber() + ":" + buffer.getNumber())
            // VERTEX RID
            .put("vertex", "#" + buffer.getNumber() + ":" + buffer.getNumber());
      }

      if (!entries.isEmpty())
        json.put("entries", entries);
    }
    return json;
  }

  @Override
  public boolean removeEntry(final int currentPosition, final int nextItemPosition) {
    int used = getUsed();
    if (used <= getContentStartPosition())
      return false;

    if (currentPosition > used)
      return false;

    if (used - nextItemPosition < 0) {
      // CORRUPTION, UPDATE THE BOUNDARIES
      setUsed(currentPosition);
      buffer.position(currentPosition);
      return false;
    }

    if (used - nextItemPosition > 0)
      // MOVE THE ENTIRE BUFFER FROM THE NEXT ITEM TO THE CURRENT ONE
      buffer.move(nextItemPosition, currentPosition, used - nextItemPosition);

    used -= nextItemPosition - currentPosition;
    setUsed(used);

    buffer.position(currentPosition);

    // DECREMENT CACHED COUNT (v1 format)
    totalCount--;
    updateCachedCount();

    return true;
  }

  @Override
  public int removeEdge(final RID rid) {
    final int contentStartPos = getContentStartPosition();
    int used = getUsed();
    if (used <= contentStartPos)
      return 0;

    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();

    buffer.position(contentStartPos);

    int found = 0;
    while (buffer.position() < used) {
      final int lastPos = buffer.position();

      final int currEdgeBucketId = (int) buffer.getNumber();
      final long currEdgePosition = buffer.getNumber();

      buffer.getNumber();
      buffer.getNumber();

      if (currEdgeBucketId == bucketId && currEdgePosition == position) {
        // FOUND MOVE THE ENTIRE BUFFER FROM THE NEXT ITEM TO THE CURRENT ONE
        buffer.move(buffer.position(), lastPos, used - buffer.position());

        used -= (buffer.position() - lastPos);
        setUsed(used);

        buffer.position(lastPos);
        ++found;

        // DECREMENT COUNT (only for v1 format)
        if (found > 0 && true) {
          totalCount--;
          updateCachedCount();
        }

        break;
      }
    }

    return found;
  }

  @Override
  public int removeVertex(final RID rid) {
    final int contentStartPos = getContentStartPosition();
    int used = getUsed();
    if (used <= contentStartPos)
      return 0;

    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();

    buffer.position(contentStartPos);

    int found = 0;
    while (buffer.position() < used) {
      final int lastPos = buffer.position();

      buffer.getNumber();
      buffer.getNumber();

      final int currVertexBucketId = (int) buffer.getNumber();
      final long currVertexPosition = buffer.getNumber();

      if (currVertexBucketId == bucketId && currVertexPosition == position) {
        // FOUND MOVE THE ENTIRE BUFFER FROM THE NEXT ITEM TO THE CURRENT ONE
        buffer.move(buffer.position(), lastPos, used - buffer.position());

        used -= (buffer.position() - lastPos);
        setUsed(used);

        buffer.position(lastPos);
        ++found;

        // DECREMENT COUNT (only for v1 format)
        if (found > 0 && true) {
          totalCount--;
          updateCachedCount();
        }

        break;
      }
    }

    return found;
  }

  @Override
  public long count() {
    long total = 0;

    final int used = getUsed();
    if (used > 0) {
      final int contentStartPos = getContentStartPosition();
      buffer.position(contentStartPos);

      while (buffer.position() < used) {
        final int fileId = (int) buffer.getNumber();
        // SKIP EDGE RID POSITION AND VERTEX RID
        buffer.getNumber();
        buffer.getNumber();
        buffer.getNumber();
        ++total;
      }
    }

    return total;
  }

  @Override
  public EdgeSegment getPrevious() {
    buffer.position(PREVIOUS_RID_POSITION);

    final RID nextRID =
        (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_RID, null); // NEXT

    if (nextRID.getBucketId() == -1 && nextRID.getPosition() == -1)
      return null;

    return (EdgeSegment) database.lookupByRID(nextRID, true);
  }

  @Override
  public Binary getContent() {
    buffer.position(bufferSize);
    buffer.flip();
    return buffer;
  }

  @Override
  public int getUsed() {
    return buffer.getInt(USED_BYTES_POSITION);
  }

  private void setUsed(final int size) {
    buffer.putInt(USED_BYTES_POSITION, size);
  }

  @Override
  public RID getRID(final AtomicInteger currentPosition) {
    buffer.position(currentPosition.get());
    final RID next = (RID) database.getSerializer()
        .deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null); // NEXT
    currentPosition.set(buffer.position());
    return next;
  }

  @Override
  public int getRecordSize() {
    return buffer.size();
  }

  @Override
  public void setIdentity(final RID rid) {
    this.rid = rid;
  }

  @Override
  public void unsetDirty() {
    // IGNORE THIS FLAG
  }

  public boolean isEmpty() {
    final int contentStartPos = getContentStartPosition();
    return getUsed() <= contentStartPos;
  }

  @Override
  public long getTotalCount() {
    return totalCount;
  }

  /**
   * Decrements the cached total count (for head segment updates from continuation segments).
   */
  public void decrementTotalCount() {
    totalCount--;
    updateCachedCount();
  }

  @Override
  public boolean isFirstSegment() {
    return true;
  }

  @Override
  public int getContentStartOffset() {
    return getContentStartPosition();
  }

  @Override
  public void setPrevious(final EdgeSegment previous) {
    final RID previousRID = previous.getIdentity();
    if (previousRID == null)
      throw new IllegalArgumentException("Previous segment is not persistent");

    buffer.position(PREVIOUS_RID_POSITION);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, buffer, BinaryTypes.TYPE_RID, previousRID);

    // When a new segment is created and linked to previous segment:
    // The new segment (this) becomes the new head with the total count
    // If both segments are v1, add the inherited count

    if (true && previous.getTotalCount() >= 0) {
      // Both are v1 format - add inherited count from previous segment
      this.totalCount += previous.getTotalCount();
      updateCachedCount();
    }
  }

  /**
   * Returns the content start position (v1 format).
   * Position is dynamic because PREVIOUS_RID is variable-length.
   */
  private int getContentStartPosition() {
    // v1 format: [RECORD_TYPE][COUNT][USED_BYTES][PREVIOUS_RID][ENTRIES...]
    // Content starts after variable-length PREVIOUS_RID
    final int savedPosition = buffer.position();
    buffer.position(PREVIOUS_RID_POSITION);
    database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_RID, null); // Skip PREVIOUS_RID
    final int contentStart = buffer.position();
    buffer.position(savedPosition); // Restore position
    return contentStart;
  }

  /**
   * Updates the cached count in the buffer (v1 format).
   */
  private void updateCachedCount() {
    // COUNT is at fixed position in v1 format
    buffer.putInt(COUNT_POSITION, (int) totalCount); // Write as unsigned int
  }

  /**
   * FOR TESTING ONLY: Set the total count directly (to test boundary conditions).
   */
  public void setTotalCountForTesting(final long count) {
    this.totalCount = count;
    updateCachedCount();
  }
}
