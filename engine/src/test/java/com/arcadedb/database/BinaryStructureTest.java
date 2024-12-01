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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class BinaryStructureTest {

  @Test
  public void write() {
    addValues(new Binary());
  }

  @Test
  public void read() {
    final Binary blob = new Binary();
    addValues(blob);

    blob.rewind();

    assertThat(blob.getByte()).isEqualTo((byte) 10);
    assertThat(blob.getShort()).isEqualTo((short) 10);
    assertThat(blob.getInt()).isEqualTo(10);
    assertThat(blob.getLong()).isEqualTo(10L);
    assertThat(blob.getString()).isEqualTo("ciao");
    assertThat(new String(blob.getBytes())).isEqualTo("ciao");
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

    int added = blob.putString("ciao");
    assertThat(added).isEqualTo(5);
    size += added;
    assertThat(blob.size()).isEqualTo(size);

    added = blob.putBytes("ciao".getBytes());
    assertThat(added).isEqualTo(5);
    size += added;
    assertThat(blob.size()).isEqualTo(size);
  }
}
