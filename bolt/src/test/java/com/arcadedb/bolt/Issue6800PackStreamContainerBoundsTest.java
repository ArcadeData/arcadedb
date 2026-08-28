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
package com.arcadedb.bolt;

import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6800: the declared element/entry/field counts of the narrow PackStream container
 * markers bypassed the size guards added for #5918, which validated only the 32-bit ones.
 * <p>
 * The amplifier was LIST_16: 1001 repetitions of the three bytes {@code D5 FF FF} declared 1001 lists of 65535
 * elements each, all live at once on the decoder's frame stack, so ~3 KB on the wire pre-authentication sized
 * ~256 MB of backing arrays before the depth guard finally fired. The identical declaration sent as LIST_32 was
 * already rejected, which is what made this a gap rather than a design choice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6800PackStreamContainerBoundsTest {

  private static final int MAX_VALUE_LENGTH = 16 * 1024 * 1024;
  private static final int MAX_ELEMENTS     = 1_048_576;
  private static final int MAX_DEPTH        = 1000;

  private static Object decode(final byte[] message) throws IOException {
    return new PackStreamReader(message, MAX_VALUE_LENGTH, MAX_ELEMENTS, MAX_DEPTH).readValue();
  }

  /**
   * The report's exact payload. It must be rejected on the very first marker, from the declared size alone:
   * reaching the depth guard at all would mean 1000 backing arrays had already been sized on the way there.
   */
  @Test
  void nestedList16IsRejectedOnTheDeclaredSizeNotOnDepth() {
    final ByteArrayOutputStream attack = new ByteArrayOutputStream();
    for (int i = 0; i < 1001; i++) {
      attack.write(0xD5); // LIST_16
      attack.write(0xFF);
      attack.write(0xFF); // 65535 declared elements, with nothing behind them
    }

    assertThatThrownBy(() -> decode(attack.toByteArray()))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("LIST_16")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  @Test
  void list8DeclaredSizeBeyondTheMessageIsRejected() {
    // LIST_8 declaring 200 elements with 3 bytes behind it.
    final byte[] message = { (byte) 0xD4, (byte) 200, 0x01, 0x02, 0x03 };

    assertThatThrownBy(() -> decode(message))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("LIST_8")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  @Test
  void tinyListDeclaredSizeBeyondTheMessageIsRejected() {
    // TINY_LIST of 15 elements with 2 bytes behind it.
    final byte[] message = { (byte) 0x9F, 0x01, 0x02 };

    assertThatThrownBy(() -> decode(message))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("TINY_LIST")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  @Test
  void map16DeclaredSizeBeyondTheMessageIsRejected() {
    final byte[] message = { (byte) 0xD9, (byte) 0xFF, (byte) 0xFF, (byte) 0x80 };

    assertThatThrownBy(() -> decode(message))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("MAP_16")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  @Test
  void string16DeclaredLengthBeyondTheMessageIsRejected() {
    final byte[] message = { (byte) 0xD1, (byte) 0xFF, (byte) 0xFF, 0x41, 0x42 };

    assertThatThrownBy(() -> decode(message))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("STRING_16")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  @Test
  void bytes8DeclaredLengthBeyondTheMessageIsRejected() {
    final byte[] message = { (byte) 0xCC, (byte) 250, 0x00 };

    assertThatThrownBy(() -> decode(message))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("BYTES_8")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  @Test
  void structFieldCountBeyondTheMessageIsRejected() {
    // TINY_STRUCT declaring 5 fields, signature 0x10 (RUN), then a single field.
    final byte[] message = { (byte) 0xB5, 0x10, 0x01 };

    assertThatThrownBy(() -> decode(message))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("TINY_STRUCT")
        .hasMessageContaining("exceeds the remaining message bytes");
  }

  /**
   * The tightened bounds must not cost a legitimate message anything: a list wide enough to need LIST_16, a map
   * wide enough to need MAP_16, and a string wide enough to need STRING_16 all still round-trip.
   */
  @Test
  @SuppressWarnings("unchecked")
  void legitimateWideContainersStillRoundTrip() throws Exception {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader((byte) 0x10, 3);

    final StringBuilder longString = new StringBuilder();
    for (int i = 0; i < 500; i++)
      longString.append('x');
    writer.writeString(longString.toString());

    final Map<String, Object> params = new java.util.LinkedHashMap<>();
    for (int i = 0; i < 300; i++)
      params.put("k" + i, (long) i);
    writer.writeMap(params);

    final List<Object> values = new java.util.ArrayList<>();
    for (int i = 0; i < 300; i++)
      values.add((long) i);
    writer.writeMap(Map.of("list", values));

    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) decode(writer.toByteArray());

    assertThat(struct.getFields()).hasSize(3);
    assertThat((String) struct.getFields().get(0)).hasSize(500);
    assertThat((Map<String, Object>) struct.getFields().get(1)).hasSize(300).containsEntry("k299", 299L);
    assertThat((List<Object>) ((Map<String, Object>) struct.getFields().get(2)).get("list")).hasSize(300);
  }

  /**
   * An empty container declares zero elements, so it must survive the guard untouched even when it is the very
   * last thing in the message and no bytes remain behind it.
   */
  @Test
  void emptyContainersAtTheEndOfAMessageAreStillAccepted() throws Exception {
    assertThat((List<?>) decode(new byte[] { (byte) 0x90 })).isEmpty();
    assertThat((Map<?, ?>) decode(new byte[] { (byte) 0xA0 })).isEmpty();
    assertThat((List<?>) decode(new byte[] { (byte) 0xD4, 0x00 })).isEmpty();
    assertThat((Map<?, ?>) decode(new byte[] { (byte) 0xD9, 0x00, 0x00 })).isEmpty();
  }
}
