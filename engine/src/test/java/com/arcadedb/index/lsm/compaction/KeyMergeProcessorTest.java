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
package com.arcadedb.index.lsm.compaction;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndexUnderlyingPageCursor;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for KeyMergeProcessor class.
 */
class KeyMergeProcessorTest extends TestHelper {

  private BinaryComparator  comparator;
  private KeyMergeProcessor processor;
  private CompactionMetrics metrics;
  private byte[]            keyTypes;

  @BeforeEach
  void setUp() {
    comparator = new BinaryComparator();
    keyTypes = new byte[] { BinaryTypes.TYPE_INT, BinaryTypes.TYPE_STRING };
    processor = new KeyMergeProcessor(comparator, keyTypes);
    metrics = new CompactionMetrics();
  }

  @Test
  void testFindMinorKeyWithNoKeys() {
    Object[][] keys = new Object[3][];
    keys[0] = null;
    keys[1] = null;
    keys[2] = null;

    KeyMergeProcessor.MinorKeyResult result = processor.findMinorKey(keys);

    assertThat(result.hasMoreItems()).isFalse();
    assertThat(result.hasMinorKey()).isFalse();
    assertThat(result.minorKey()).isNull();
    assertThat(result.iteratorIndexes()).isEmpty();
  }

  @Test
  void testFindMinorKeyWithSingleKey() {
    Object[][] keys = new Object[3][];
    keys[0] = new Object[] { 1, "apple" };
    keys[1] = null;
    keys[2] = null;

    KeyMergeProcessor.MinorKeyResult result = processor.findMinorKey(keys);

    assertThat(result.hasMoreItems()).isTrue();
    assertThat(result.hasMinorKey()).isTrue();
    assertThat(result.minorKey()).isEqualTo(keys[0]);
    assertThat(result.iteratorIndexes()).isEqualTo(List.of(0));
  }

  @Test
  void testFindMinorKeyWithMultipleKeys() {
    Object[][] keys = new Object[3][];
    keys[0] = new Object[] { 3, "cherry" };
    keys[1] = new Object[] { 1, "apple" };
    keys[2] = new Object[] { 2, "banana" };

    // The real BinaryComparator will properly compare these keys
    // keys[1] < keys[2] < keys[0] based on integer comparison
    KeyMergeProcessor.MinorKeyResult result = processor.findMinorKey(keys);

    assertThat(result.hasMoreItems()).isTrue();
    assertThat(result.hasMinorKey()).isTrue();
    assertThat(result.minorKey()).isEqualTo(keys[1]); // Should be the smallest key
    assertThat(result.iteratorIndexes()).isEqualTo(List.of(1));
  }

  @Test
  void testFindMinorKeyWithDuplicateKeys() {
    Object[][] keys = new Object[3][];
    keys[0] = new Object[] { 1, "apple" };
    keys[1] = new Object[] { 1, "apple" };
    keys[2] = new Object[] { 2, "banana" };

    // The real BinaryComparator will handle equality and ordering properly
    KeyMergeProcessor.MinorKeyResult result = processor.findMinorKey(keys);

    assertThat(result.hasMoreItems()).isTrue();
    assertThat(result.hasMinorKey()).isTrue();
    assertThat(result.minorKey()).isEqualTo(keys[0]);
    assertThat(result.iteratorIndexes()).isEqualTo(List.of(0, 1));
  }

  @Test
  void testMergeKeyWithNullIterators() {
    Object[] minorKey = new Object[] { 1, "apple" };
    List<Integer> iteratorIndexes = List.of(0);
    LSMTreeIndexUnderlyingPageCursor[] iterators = { null, null, null };
    Object[][] keys = new Object[3][];

    KeyMergeProcessor.MergeResult result = processor.mergeKey(
        minorKey, iteratorIndexes, iterators, keys, metrics);

    // With null iterators, should return empty result
    assertThat(result.isEmpty()).isTrue();
    assertThat(result.rids()).hasSize(0);
  }

  @Test
  void testMergeKeyWithEmptyIndexes() {
    Object[] minorKey = new Object[] { 1, "apple" };
    List<Integer> iteratorIndexes = List.of(); // Empty list
    LSMTreeIndexUnderlyingPageCursor[] iterators = { null, null, null };
    Object[][] keys = new Object[3][];

    KeyMergeProcessor.MergeResult result = processor.mergeKey(
        minorKey, iteratorIndexes, iterators, keys, metrics);

    assertThat(result.isEmpty()).isTrue();
    assertThat(result.rids()).hasSize(0);
  }

  @Test
  void testMergeKeyWithOutOfBoundsIndexes() {
    Object[] minorKey = new Object[] { 1, "apple" };
    List<Integer> iteratorIndexes = List.of(5); // Index out of bounds
    LSMTreeIndexUnderlyingPageCursor[] iterators = { null, null, null };
    Object[][] keys = new Object[3][];

    // The implementation doesn't handle out of bounds gracefully, it throws exception
    assertThatThrownBy(() -> {
      processor.mergeKey(minorKey, iteratorIndexes, iterators, keys, metrics);
    }).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  void testMergeKeyWithNegativeIndexes() {
    Object[] minorKey = new Object[] { 1, "apple" };
    List<Integer> iteratorIndexes = List.of(-1); // Negative index
    LSMTreeIndexUnderlyingPageCursor[] iterators = { null, null, null };
    Object[][] keys = new Object[3][];

    // The implementation doesn't handle negative indexes gracefully, it throws exception
    assertThatThrownBy(() -> {
      processor.mergeKey(minorKey, iteratorIndexes, iterators, keys, metrics);
    }).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  void testCurrentRidCount() {
    assertThat(processor.getCurrentRidCount()).isEqualTo(0);
  }

  @Test
  void testClearRidBuffer() {
    // This test is mainly to ensure the method exists and doesn't throw
    assertThatCode(() -> processor.clearRidBuffer()).doesNotThrowAnyException();
    assertThat(processor.getCurrentRidCount()).isEqualTo(0);
  }

  @Test
  void testMinorKeyResultNoMoreItems() {
    KeyMergeProcessor.MinorKeyResult result = KeyMergeProcessor.MinorKeyResult.noMoreItems();

    assertThat(result.hasMoreItems()).isFalse();
    assertThat(result.hasMinorKey()).isFalse();
    assertThat(result.minorKey()).isNull();
    assertThat(result.iteratorIndexes()).isEmpty();
  }

  @Test
  void testMergeResultEmpty() {
    KeyMergeProcessor.MergeResult result = KeyMergeProcessor.MergeResult.empty();

    assertThat(result.isEmpty()).isTrue();
    assertThat(result.rids()).hasSize(0);
  }

  @Test
  void testMergeResultOf() {
    RID[] rids = { new RID(null, 1, 1), new RID(null, 1, 2) };
    KeyMergeProcessor.MergeResult result = KeyMergeProcessor.MergeResult.of(rids);

    assertThat(result.isEmpty()).isFalse();
    assertThat(result.rids()).isEqualTo(rids);
  }

  @Test
  void testMergeResultOfEmpty() {
    RID[] rids = new RID[0];
    KeyMergeProcessor.MergeResult result = KeyMergeProcessor.MergeResult.of(rids);

    assertThat(result.isEmpty()).isTrue();
    assertThat(result.rids()).hasSize(0);
  }
}
