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

import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10856: the bulk float and double reads take their element count from the record, so a corrupt count must be
 * refused without moving the position, both when the backing array ends with the content and when it has spare capacity
 * past it, whose bytes are not content.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10856BinaryBulkArrayBoundsTest {

  @Test
  void aFloatCountPastTheContentIsRefusedWithSpareCapacity() {
    final Binary buffer = new Binary(1024);
    buffer.putFloatArray(new float[] { 1f, 2f, 3f });
    buffer.position(0);

    assertThatThrownBy(() -> buffer.getFloatArray(4)).isInstanceOf(BufferUnderflowException.class);
    assertThat(buffer.position()).isZero();
    assertThat(buffer.getFloatArray(3)).containsExactly(1f, 2f, 3f);
  }

  @Test
  void aDoubleCountPastTheContentIsRefusedWithSpareCapacity() {
    final Binary buffer = new Binary(1024);
    buffer.putDoubleArray(new double[] { 1d, 2d });
    buffer.position(0);

    assertThatThrownBy(() -> buffer.getDoubleArray(3)).isInstanceOf(BufferUnderflowException.class);
    assertThat(buffer.position()).isZero();
    assertThat(buffer.getDoubleArray(2)).containsExactly(1d, 2d);
  }

  @Test
  void aCountPastAnExactBackingArrayIsRefused() {
    final Binary written = new Binary(64);
    written.putFloatArray(new float[] { 1f, 2f });
    final Binary buffer = new Binary(Arrays.copyOf(written.getContent(), written.size()));

    assertThatThrownBy(() -> buffer.getFloatArray(3)).isInstanceOf(BufferUnderflowException.class);
    assertThat(buffer.position()).isZero();
    assertThatThrownBy(() -> buffer.getDoubleArray(2)).isInstanceOf(BufferUnderflowException.class);
    assertThat(buffer.position()).isZero();
  }

  @Test
  void aNegativeOrOverflowingCountIsRefused() {
    final Binary buffer = new Binary(64);
    buffer.putFloatArray(new float[] { 1f });
    buffer.position(0);

    assertThatThrownBy(() -> buffer.getFloatArray(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> buffer.getDoubleArray(Integer.MAX_VALUE)).isInstanceOf(IllegalArgumentException.class);
    assertThat(buffer.position()).isZero();
  }
}
