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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for VectorSerializer binary serialization/deserialization.
 *
 * <p>Tests cover:
 * - Basic vector serialization and deserialization
 * - RID set serialization and deserialization
 * - Combined vector entry serialization/deserialization
 * - Edge cases (empty vectors, single RID, many RIDs)
 * - Special float values (NaN, Infinity, Zero, Negative)
 * - Size calculation accuracy
 * - Round-trip serialization consistency
 *
 * @author Arcade Data
 * @since 25.11.0
 */
public class VectorSerializerTest extends TestHelper {

  /**
   * Test basic vector serialization and deserialization.
   */
  @Test
  public void testSerializeDeserializeVector() {
    final float[] vector = {1.0f, 2.5f, 3.14f, -4.2f, 0.0f};
    final Binary buffer = new Binary(VectorSerializer.calculateVectorSize(vector.length));

    // Serialize
    VectorSerializer.serializeVector(buffer, vector);

    // Reset buffer position for reading
    buffer.rewind();

    // Deserialize
    final float[] deserialized = VectorSerializer.deserializeVector(buffer);

    // Verify
    assertThat(deserialized).isNotNull();
    assertThat(deserialized).hasSameSizeAs(vector);
    for (int i = 0; i < vector.length; i++) {
      assertThat(deserialized[i]).isEqualTo(vector[i]);
    }
  }

  /**
   * Test vector serialization with special float values.
   */
  @Test
  public void testVectorWithSpecialFloatValues() {
    final float[] vector = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, -0.0f, 1.0f};
    final Binary buffer = new Binary(VectorSerializer.calculateVectorSize(vector.length));

    // Serialize
    VectorSerializer.serializeVector(buffer, vector);
    buffer.rewind();

    // Deserialize
    final float[] deserialized = VectorSerializer.deserializeVector(buffer);

    // Verify - special values should be preserved exactly
    assertThat(deserialized).hasSameSizeAs(vector);
    assertThat(Float.isNaN(deserialized[0])).isTrue();
    assertThat(Float.isInfinite(deserialized[1])).isTrue();
    assertThat(deserialized[1] > 0).isTrue(); // POSITIVE_INFINITY
    assertThat(Float.isInfinite(deserialized[2])).isTrue();
    assertThat(deserialized[2] < 0).isTrue(); // NEGATIVE_INFINITY
    assertThat(deserialized[3]).isEqualTo(0.0f);
    assertThat(deserialized[4]).isEqualTo(-0.0f);
    assertThat(deserialized[5]).isEqualTo(1.0f);
  }

  /**
   * Test RID set serialization and deserialization.
   */
  @Test
  public void testSerializeDeserializeRIDSet() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final Set<RID> rids = new HashSet<>();
    rids.add(new RID(dbInternal, 1, 100L));
    rids.add(new RID(dbInternal, 2, 200L));
    rids.add(new RID(dbInternal, 3, 300L));

    final Binary buffer = new Binary(VectorSerializer.calculateRIDSetSize(rids.size()));

    // Serialize
    VectorSerializer.serializeRIDSet(buffer, rids);
    buffer.rewind();

    // Deserialize
    final Set<RID> deserialized = VectorSerializer.deserializeRIDSet(buffer, dbInternal);

    // Verify
    assertThat(deserialized).isNotNull();
    assertThat(deserialized).hasSize(rids.size());
    for (final RID rid : rids) {
      assertThat(deserialized).anyMatch(
          r -> r.getBucketId() == rid.getBucketId() && r.getPosition() == rid.getPosition());
    }
  }

  /**
   * Test combined vector entry serialization/deserialization.
   */
  @Test
  public void testSerializeDeserializeVectorEntry() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final Set<RID> rids = new HashSet<>();
    rids.add(new RID(dbInternal, 1, 10L));
    rids.add(new RID(dbInternal, 2, 20L));

    final int entrySize = VectorSerializer.calculateEntrySize(vector, rids.size());
    final Binary buffer = new Binary(entrySize);

    // Serialize
    VectorSerializer.serializeVectorEntry(buffer, vector, rids);
    buffer.rewind();

    // Deserialize
    final Object[] result = VectorSerializer.deserializeVectorEntry(buffer, dbInternal);
    final float[] deserializedVector = (float[]) result[0];
    final Set<RID> deserializedRids = (Set<RID>) result[1];

    // Verify vector
    assertThat(deserializedVector).isNotNull();
    assertThat(deserializedVector).hasSameSizeAs(vector);
    for (int i = 0; i < vector.length; i++) {
      assertThat(deserializedVector[i]).isEqualTo(vector[i]);
    }

    // Verify RIDs
    assertThat(deserializedRids).hasSize(rids.size());
    for (final RID rid : rids) {
      assertThat(deserializedRids).anyMatch(
          r -> r.getBucketId() == rid.getBucketId() && r.getPosition() == rid.getPosition());
    }
  }

  /**
   * Test serialization with single RID.
   */
  @Test
  public void testVectorEntryWithSingleRID() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final float[] vector = {1.5f, 2.5f, 3.5f};
    final Set<RID> rids = new HashSet<>();
    rids.add(new RID(dbInternal, 42, 999L));

    final int entrySize = VectorSerializer.calculateEntrySize(vector, rids.size());
    final Binary buffer = new Binary(entrySize);

    VectorSerializer.serializeVectorEntry(buffer, vector, rids);
    buffer.rewind();

    final Object[] result = VectorSerializer.deserializeVectorEntry(buffer, dbInternal);
    final Set<RID> deserializedRids = (Set<RID>) result[1];

    assertThat(deserializedRids).hasSize(1);
    final RID rid = deserializedRids.iterator().next();
    assertThat(rid.getBucketId()).isEqualTo(42);
    assertThat(rid.getPosition()).isEqualTo(999L);
  }

  /**
   * Test serialization with many RIDs.
   */
  @Test
  public void testVectorEntryWithManyRIDs() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final float[] vector = {1.0f};
    final Set<RID> rids = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      rids.add(new RID(dbInternal, i % 10, (long) i * 100));
    }

    final int entrySize = VectorSerializer.calculateEntrySize(vector, rids.size());
    final Binary buffer = new Binary(entrySize);

    VectorSerializer.serializeVectorEntry(buffer, vector, rids);
    buffer.rewind();

    final Object[] result = VectorSerializer.deserializeVectorEntry(buffer, dbInternal);
    final Set<RID> deserializedRids = (Set<RID>) result[1];

    assertThat(deserializedRids).hasSize(100);
  }

  /**
   * Test size calculation for vector.
   */
  @Test
  public void testCalculateVectorSize() {
    // Format: 2 bytes (size) + vectorSize * 4 bytes (floats)
    assertThat(VectorSerializer.calculateVectorSize(1)).isEqualTo(2 + 1 * 4);
    assertThat(VectorSerializer.calculateVectorSize(10)).isEqualTo(2 + 10 * 4);
    assertThat(VectorSerializer.calculateVectorSize(512)).isEqualTo(2 + 512 * 4);
    assertThat(VectorSerializer.calculateVectorSize(4096)).isEqualTo(2 + 4096 * 4);
  }

  /**
   * Test size calculation for RID set.
   */
  @Test
  public void testCalculateRIDSetSize() {
    // Format: 2 bytes (count) + ridCount * 12 bytes (4 bucket + 8 position)
    assertThat(VectorSerializer.calculateRIDSetSize(0)).isEqualTo(2);
    assertThat(VectorSerializer.calculateRIDSetSize(1)).isEqualTo(2 + 1 * 12);
    assertThat(VectorSerializer.calculateRIDSetSize(10)).isEqualTo(2 + 10 * 12);
    assertThat(VectorSerializer.calculateRIDSetSize(100)).isEqualTo(2 + 100 * 12);
  }

  /**
   * Test size calculation for complete entry.
   */
  @Test
  public void testCalculateEntrySize() {
    // Format: vectorSize:2 + vector:N*4 + ridCount:2 + rids:M*12
    final float[] vector = new float[10];
    final int ridCount = 5;
    final int expected = 2 + (10 * 4) + 2 + (5 * 12);

    assertThat(VectorSerializer.calculateEntrySize(vector, ridCount)).isEqualTo(expected);
  }

  /**
   * Test null vector throws exception.
   */
  @Test
  public void testSerializeVectorNullThrowsException() {
    final Binary buffer = new Binary(100);

    assertThatThrownBy(() -> VectorSerializer.serializeVector(buffer, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Vector cannot be null or empty");
  }

  /**
   * Test empty vector throws exception.
   */
  @Test
  public void testSerializeEmptyVectorThrowsException() {
    final Binary buffer = new Binary(100);
    final float[] emptyVector = new float[0];

    assertThatThrownBy(() -> VectorSerializer.serializeVector(buffer, emptyVector))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Vector cannot be null or empty");
  }

  /**
   * Test null vector in vector entry throws exception.
   */
  @Test
  public void testSerializeVectorEntryWithNullVectorThrowsException() {
    final Binary buffer = new Binary(100);
    final Set<RID> rids = new HashSet<>();

    assertThatThrownBy(() -> VectorSerializer.serializeVectorEntry(buffer, null, rids))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Vector cannot be null or empty");
  }

  /**
   * Test vector entry with empty vector throws exception.
   */
  @Test
  public void testSerializeVectorEntryWithEmptyVectorThrowsException() {
    final Binary buffer = new Binary(100);
    final float[] emptyVector = new float[0];
    final Set<RID> rids = new HashSet<>();

    assertThatThrownBy(() -> VectorSerializer.serializeVectorEntry(buffer, emptyVector, rids))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Vector cannot be null or empty");
  }

  /**
   * Test vector entry with empty RID set.
   */
  @Test
  public void testVectorEntryWithEmptyRIDSet() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final Set<RID> emptyRids = new HashSet<>();

    final int entrySize = VectorSerializer.calculateEntrySize(vector, emptyRids.size());
    final Binary buffer = new Binary(entrySize);

    VectorSerializer.serializeVectorEntry(buffer, vector, emptyRids);
    buffer.rewind();

    final Object[] result = VectorSerializer.deserializeVectorEntry(buffer, dbInternal);
    final float[] deserializedVector = (float[]) result[0];
    final Set<RID> deserializedRids = (Set<RID>) result[1];

    assertThat(deserializedVector).hasSameSizeAs(vector);
    assertThat(deserializedRids).isEmpty();
  }

  /**
   * Test invalid vector size during deserialization.
   */
  @Test
  public void testDeserializeVectorInvalidSize() {
    final Binary buffer = new Binary(10);
    buffer.putShort((short) 0); // Invalid size (0 or negative)
    buffer.rewind();

    assertThatThrownBy(() -> VectorSerializer.deserializeVector(buffer))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid vector size");
  }

  /**
   * Test invalid vector size in vector entry during deserialization.
   */
  @Test
  public void testDeserializeVectorEntryInvalidVectorSize() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final Binary buffer = new Binary(10);
    buffer.putShort((short) -5); // Invalid size
    buffer.rewind();

    assertThatThrownBy(() -> VectorSerializer.deserializeVectorEntry(buffer, dbInternal))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid vector size");
  }

  /**
   * Test round-trip serialization with large vector.
   */
  @Test
  public void testRoundTripLargeVector() {
    final float[] vector = new float[4096]; // Max vector size
    for (int i = 0; i < vector.length; i++) {
      vector[i] = (float) Math.sin(i * 0.01);
    }

    final Binary buffer = new Binary(VectorSerializer.calculateVectorSize(vector.length));

    VectorSerializer.serializeVector(buffer, vector);
    buffer.rewind();

    final float[] deserialized = VectorSerializer.deserializeVector(buffer);

    assertThat(deserialized).hasSameSizeAs(vector);
    for (int i = 0; i < vector.length; i++) {
      assertThat(deserialized[i]).isEqualTo(vector[i]);
    }
  }

  /**
   * Test round-trip serialization with high bucket IDs and positions.
   */
  @Test
  public void testRoundTripLargeRIDValues() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final float[] vector = {1.0f};
    final Set<RID> rids = new HashSet<>();
    rids.add(new RID(dbInternal, Integer.MAX_VALUE - 1, Long.MAX_VALUE - 1));
    rids.add(new RID(dbInternal, 0, 0L));

    final int entrySize = VectorSerializer.calculateEntrySize(vector, rids.size());
    final Binary buffer = new Binary(entrySize);

    VectorSerializer.serializeVectorEntry(buffer, vector, rids);
    buffer.rewind();

    final Object[] result = VectorSerializer.deserializeVectorEntry(buffer, dbInternal);
    final Set<RID> deserializedRids = (Set<RID>) result[1];

    assertThat(deserializedRids).hasSize(2);
    for (final RID rid : rids) {
      assertThat(deserializedRids).anyMatch(
          r -> r.getBucketId() == rid.getBucketId() && r.getPosition() == rid.getPosition());
    }
  }

  /**
   * Test vector precision is maintained (IEEE 754 round-trip).
   */
  @Test
  public void testVectorPrecisionMaintained() {
    // Test various floating point values that might cause precision issues
    final float[] vector = {
        0.1f + 0.2f,           // Classic floating point issue
        1.0f / 3.0f,           // 0.333...
        (float) Math.PI,       // Pi
        (float) Math.E,        // Euler's number
        Float.MIN_VALUE,       // Smallest positive value
        Float.MAX_VALUE        // Largest value
    };

    final Binary buffer = new Binary(VectorSerializer.calculateVectorSize(vector.length));
    VectorSerializer.serializeVector(buffer, vector);
    buffer.rewind();

    final float[] deserialized = VectorSerializer.deserializeVector(buffer);

    // Values should match exactly (bitwise)
    for (int i = 0; i < vector.length; i++) {
      assertThat(Float.floatToIntBits(deserialized[i]))
          .isEqualTo(Float.floatToIntBits(vector[i]));
    }
  }

  /**
   * Test RID set ordering doesn't affect deserialization.
   */
  @Test
  public void testRIDSetOrderIndependent() {
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    final float[] vector = {1.0f};

    // Create first RID set with specific order
    final Set<RID> rids1 = new HashSet<>();
    rids1.add(new RID(dbInternal, 1, 10L));
    rids1.add(new RID(dbInternal, 2, 20L));
    rids1.add(new RID(dbInternal, 3, 30L));

    // Serialize and deserialize
    final int entrySize = VectorSerializer.calculateEntrySize(vector, rids1.size());
    final Binary buffer = new Binary(entrySize);
    VectorSerializer.serializeVectorEntry(buffer, vector, rids1);
    buffer.rewind();

    final Object[] result = VectorSerializer.deserializeVectorEntry(buffer, dbInternal);
    final Set<RID> deserializedRids = (Set<RID>) result[1];

    // Verify all RIDs are present regardless of order
    assertThat(deserializedRids).hasSize(3);
    assertThat(deserializedRids).anyMatch(r -> r.getBucketId() == 1 && r.getPosition() == 10L);
    assertThat(deserializedRids).anyMatch(r -> r.getBucketId() == 2 && r.getPosition() == 20L);
    assertThat(deserializedRids).anyMatch(r -> r.getBucketId() == 3 && r.getPosition() == 30L);
  }
}
