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

import static org.assertj.core.api.Assertions.assertThat;

class BinaryStructureTest {

  @Test
  void write() {
    addValues(new Binary());
  }

  @Test
  void read() {
    final Binary blob = new Binary();
    addValues(blob);

    blob.rewind();

    assertThat(blob.getByte()).isEqualTo((byte) 10);
    assertThat(blob.getShort()).isEqualTo((short) 10);
    assertThat(blob.getInt()).isEqualTo(10);
    assertThat(blob.getLong()).isEqualTo(10L);
    assertThat(blob.getNumber()).isEqualTo(10L);
    assertThat(blob.getString()).isEqualTo("ciao");
    assertThat(new String(blob.getBytes())).isEqualTo("ciao");
    final byte[] bytes = new byte[4];
    blob.getByteArray(bytes);
    assertThat(new String(bytes)).isEqualTo("ciao");
  }

  /**
   * #6217: the bulk region comparison a chunked read validates itself with. The properties that matter to it are
   * that both indexes are ABSOLUTE and independent (the same bytes at different offsets in the two buffers must
   * compare equal), that neither buffer's position moves, and that a buffer whose content does not start at array
   * index 0 - which is what a page view is - is compared from where its content really begins.
   */
  @Test
  void isSameRegionAs() {
    final Binary one = new Binary("--hello world--".getBytes());
    final Binary other = new Binary("hello world".getBytes());

    one.position(3);
    other.position(7);

    assertThat(one.isSameRegionAs(2, other, 0, 11)).isTrue();
    assertThat(one.isSameRegionAs(2, other, 0, 0)).as("an empty region is the same as any other").isTrue();
    assertThat(one.isSameRegionAs(8, other, 6, 5)).as("the same bytes at different offsets").isTrue();
    assertThat(one.isSameRegionAs(2, other, 1, 10)).as("shifted by one, so 'hello worl' against 'ello world'").isFalse();
    assertThat(one.isSameRegionAs(0, other, 0, 11)).isFalse();

    assertThat(one.position()).as("comparing must not move either position").isEqualTo(3);
    assertThat(other.position()).isEqualTo(7);

    // A buffer whose content begins past the start of its array, as every page view is.
    final Binary sliced = new Binary(java.nio.ByteBuffer.wrap("--hello world".getBytes()).position(2).slice());
    assertThat(sliced.isSameRegionAs(0, other, 0, 11)).isTrue();
    assertThat(other.isSameRegionAs(0, sliced, 0, 11)).isTrue();
  }

  private void addValues(final BinaryStructure blob) {
    int size = 0;
    assertThat(blob.size()).isEqualTo(size);

    blob.putByte((byte) 10);
    size += Binary.BYTE_SERIALIZED_SIZE;
    assertThat(blob.size()).isEqualTo(size);

    blob.putShort((short) 10);
    size += Binary.SHORT_SERIALIZED_SIZE;
    assertThat(blob.size()).isEqualTo(size);

    blob.putInt(10);
    size += Binary.INT_SERIALIZED_SIZE;
    assertThat(blob.size()).isEqualTo(size);

    blob.putLong(10L);
    size += Binary.LONG_SERIALIZED_SIZE;
    assertThat(blob.size()).isEqualTo(size);

    int added = blob.putNumber(10);
    assertThat(added).isEqualTo(1);
    size += added;
    assertThat(blob.size()).isEqualTo(size);

    added = blob.putString("ciao");
    assertThat(added).isEqualTo(5);
    size += added;
    assertThat(blob.size()).isEqualTo(size);

    added = blob.putBytes("ciao".getBytes());
    assertThat(added).isEqualTo(5);
    size += added;
    assertThat(blob.size()).isEqualTo(size);

    blob.putByteArray("ciao".getBytes());
    size += 4;
    assertThat(blob.size()).isEqualTo(size);
  }
}
