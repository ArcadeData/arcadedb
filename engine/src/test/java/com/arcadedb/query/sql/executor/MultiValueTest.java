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

import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.utility.MultiIterator;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MultiValueTest {
  @Test
  void multivaluesClasses() {
    assertThat(MultiValue.isMultiValue(Map.class)).isTrue();
    assertThat(MultiValue.isMultiValue(List.class)).isTrue();
    assertThat(MultiValue.isMultiValue(Set.class)).isTrue();
    assertThat(MultiValue.isMultiValue(Collection.class)).isTrue();
    assertThat(MultiValue.isMultiValue(Object[].class)).isTrue();
    assertThat(MultiValue.isMultiValue(Iterable.class)).isTrue();
    assertThat(MultiValue.isMultiValue(MultiIterator.class)).isTrue();
    assertThat(MultiValue.isMultiValue(ResultSet.class)).isTrue();
  }

  @Test
  void multivaluesObjects() {
    assertThat(MultiValue.isMultiValue(Map.of())).isTrue();
    assertThat(MultiValue.isMultiValue(List.of())).isTrue();
    assertThat(MultiValue.isMultiValue(Set.of())).isTrue();
    assertThat(MultiValue.isMultiValue(new Object[] {})).isTrue();
    //iterable
    assertThat(MultiValue.isMultiValue(new EmptyIndexCursor())).isTrue();
    assertThat(MultiValue.isMultiValue(new MultiIterator())).isTrue();
    assertThat(MultiValue.isMultiValue(new InternalResultSet())).isTrue();
  }

  @Test
  void multivaluesSize() {

    assertThat(MultiValue.getSize(null)).isEqualTo(0);
    assertThat(MultiValue.getSize("single")).isEqualTo(0);
    assertThat(MultiValue.getSize(Map.of("key", "value"))).isEqualTo(1);
    assertThat(MultiValue.getSize(List.of("one"))).isEqualTo(1);
    assertThat(MultiValue.getSize(Set.of("one", "two"))).isEqualTo(2);
    assertThat(MultiValue.getSize(new Object[] {})).isEqualTo(0);
    //iterable
    assertThat(MultiValue.getSize(new EmptyIndexCursor())).isEqualTo(0);
    assertThat(MultiValue.getSize(new MultiIterator())).isEqualTo(0);
    assertThat(MultiValue.getSize(new InternalResultSet())).isEqualTo(0);
  }

  /**
   * Regression test for #7910: {@code isSequenceArray()} is the single source of truth telling {@code ExpandStep} and
   * {@code UnwindStep} which arrays are a sequence of values. Only the primitive {@code byte[]} is excluded, because
   * that is how a {@code BINARY} property - an opaque blob - is represented; the BOXED {@code Byte[]} is an ordinary
   * array and must keep expanding element by element.
   */
  @Test
  void sequenceArraysExcludeOnlyTheBinaryBlob() {
    assertThat(MultiValue.isSequenceArray(new float[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new double[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new long[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new int[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new short[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new Object[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new Byte[] { 1, 2 })).isTrue();
    assertThat(MultiValue.isSequenceArray(new byte[][] { { 1 } })).isTrue();

    assertThat(MultiValue.isSequenceArray(new byte[] { 1, 2 })).isFalse();
    assertThat(MultiValue.isSequenceArray(null)).isFalse();
    assertThat(MultiValue.isSequenceArray("a string")).isFalse();
    assertThat(MultiValue.isSequenceArray(List.of(1, 2))).isFalse();
    assertThat(MultiValue.isSequenceArray(Map.of("k", 1))).isFalse();
  }
}
