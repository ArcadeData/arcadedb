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
package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for serializing and deserializing vector data to/from binary buffers.
 *
 * <p><b>Binary Format:</b>
 * <pre>
 * VectorEntry = [vectorSize:2] [vector:vectorSize*4] [ridCount:2] [rids:ridCount*12]
 * RID = [bucketId:4] [position:8]
 * </pre>
 *
 * @author Arcade Data
 * @since 25.11.0
 */
public class VectorSerializer {

  /**
   * Calculate the size needed to store a vector entry (vector + RID set) in binary form.
   *
   * @param vector the vector array
   * @param ridCount number of RIDs to store
   * @return size in bytes
   */
  public static int calculateEntrySize(final float[] vector, final int ridCount) {
    // vectorSize:2 + vector:N*4 + ridCount:2 + rids:M*12
    return 2 + (vector.length * 4) + 2 + (ridCount * 12);
  }

  /**
   * Serialize a vector and its RID set to a binary buffer.
   *
   * @param buffer the binary buffer to write to
   * @param vector the vector to serialize
   * @param rids the set of RIDs associated with this vector
   */
  public static void serializeVectorEntry(final Binary buffer, final float[] vector, final Set<RID> rids) {
    if (vector == null || vector.length == 0)
      throw new IllegalArgumentException("Vector cannot be null or empty");

    // Write vector size (short)
    buffer.putShort((short) vector.length);

    // Write vector values (float = 4 bytes each)
    for (final float value : vector) {
      buffer.putInt(Float.floatToIntBits(value));
    }

    // Write RID count (short)
    buffer.putShort((short) rids.size());

    // Write RIDs (12 bytes each: 4 bytes bucket ID + 8 bytes position)
    for (final RID rid : rids) {
      buffer.putInt(rid.getBucketId());
      buffer.putLong(rid.getPosition());
    }
  }

  /**
   * Deserialize a vector entry from a binary buffer.
   *
   * @param buffer the binary buffer to read from
   * @param database the database instance (for creating RIDs)
   * @return array containing [float[] vector, Set&lt;RID&gt; rids]
   */
  public static Object[] deserializeVectorEntry(final Binary buffer, final com.arcadedb.database.DatabaseInternal database) {
    // Read vector size
    final short vectorSize = buffer.getShort();
    if (vectorSize <= 0)
      throw new IllegalStateException("Invalid vector size: " + vectorSize);

    // Read vector values
    final float[] vector = new float[vectorSize];
    for (int i = 0; i < vectorSize; i++) {
      vector[i] = Float.intBitsToFloat(buffer.getInt());
    }

    // Read RID count
    final short ridCount = buffer.getShort();
    final Set<RID> rids = new HashSet<>(ridCount);

    // Read RIDs
    for (int i = 0; i < ridCount; i++) {
      final int bucketId = buffer.getInt();
      final long position = buffer.getLong();
      rids.add(new RID(database, bucketId, position));
    }

    return new Object[]{vector, rids};
  }

  /**
   * Serialize just a vector (without RIDs) to a binary buffer.
   *
   * @param buffer the binary buffer to write to
   * @param vector the vector to serialize
   */
  public static void serializeVector(final Binary buffer, final float[] vector) {
    if (vector == null || vector.length == 0)
      throw new IllegalArgumentException("Vector cannot be null or empty");

    // Write vector size (short)
    buffer.putShort((short) vector.length);

    // Write vector values (float = 4 bytes each)
    for (final float value : vector) {
      buffer.putInt(Float.floatToIntBits(value));
    }
  }

  /**
   * Deserialize just a vector (without RIDs) from a binary buffer.
   *
   * @param buffer the binary buffer to read from
   * @return the vector array
   */
  public static float[] deserializeVector(final Binary buffer) {
    // Read vector size
    final short vectorSize = buffer.getShort();
    if (vectorSize <= 0)
      throw new IllegalStateException("Invalid vector size: " + vectorSize);

    // Read vector values
    final float[] vector = new float[vectorSize];
    for (int i = 0; i < vectorSize; i++) {
      vector[i] = Float.intBitsToFloat(buffer.getInt());
    }

    return vector;
  }

  /**
   * Serialize just an RID set to a binary buffer.
   *
   * @param buffer the binary buffer to write to
   * @param rids the set of RIDs to serialize
   */
  public static void serializeRIDSet(final Binary buffer, final Set<RID> rids) {
    // Write RID count (short)
    buffer.putShort((short) rids.size());

    // Write RIDs (12 bytes each: 4 bytes bucket ID + 8 bytes position)
    for (final RID rid : rids) {
      buffer.putInt(rid.getBucketId());
      buffer.putLong(rid.getPosition());
    }
  }

  /**
   * Deserialize just an RID set from a binary buffer.
   *
   * @param buffer the binary buffer to read from
   * @param database the database instance (for creating RIDs)
   * @return set of RIDs
   */
  public static Set<RID> deserializeRIDSet(final Binary buffer, final com.arcadedb.database.DatabaseInternal database) {
    // Read RID count
    final short ridCount = buffer.getShort();
    final Set<RID> rids = new HashSet<>(ridCount);

    // Read RIDs
    for (int i = 0; i < ridCount; i++) {
      final int bucketId = buffer.getInt();
      final long position = buffer.getLong();
      rids.add(new RID(database, bucketId, position));
    }

    return rids;
  }

  /**
   * Calculate the size of a serialized RID set.
   *
   * @param ridCount number of RIDs
   * @return size in bytes (2 for count + ridCount*12)
   */
  public static int calculateRIDSetSize(final int ridCount) {
    return 2 + (ridCount * 12);
  }

  /**
   * Calculate the size of a serialized vector (without RIDs).
   *
   * @param vectorSize number of dimensions
   * @return size in bytes (2 + vectorSize*4)
   */
  public static int calculateVectorSize(final int vectorSize) {
    return 2 + (vectorSize * 4);
  }
}
