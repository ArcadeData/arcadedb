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
package com.arcadedb.studio;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that Studio's binary assets survive the build intact.
 * <p>
 * They did not. Resource filtering is on for {@code src/main/resources} so two text resources can interpolate
 * build properties, and filtering copies a file through a Reader - which destroys a binary. Maven itself was
 * told to skip the binary types, so released artifacts were always correct; an IDE that copies resources on its
 * own honours {@code <filtering>} but not the plugin setting, and text-filtered the webfonts into garbage:
 * {@code fa-solid-900.woff2} became 215,949 bytes instead of 119,488, the browser refused it with "Invalid font
 * data", and every icon in Studio rendered as an empty box. The carve-out now lives in the resource definition
 * itself, where every tool reads it.
 * <p>
 * This test reads the assets off the CLASSPATH rather than from {@code src}, so what it checks is the copy that
 * was actually produced - the only place the corruption was ever visible. A file's own header is what proves it:
 * a WOFF2 records its total length in bytes 8-11, and a text filter that re-encodes the file leaves that field
 * disagreeing with the size on disk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StudioBinaryResourceIntegrityTest {

  /** Every webfont webpack copies into the Studio jar. */
  private static final List<String> WEBFONTS = List.of(
      "static/dist/webfonts/fa-brands-400.woff2",
      "static/dist/webfonts/fa-regular-400.woff2",
      "static/dist/webfonts/fa-solid-900.woff2",
      "static/dist/webfonts/fa-v4compatibility.woff2");

  @Test
  void everyWebfontIsAWellFormedWoff2() throws IOException {
    for (final String resource : WEBFONTS) {
      final byte[] font = read(resource);

      assertThat(new String(font, 0, 4, StandardCharsets.US_ASCII))
          .as("%s must start with the WOFF2 signature", resource).isEqualTo("wOF2");

      // Bytes 8-11 are the font's own total length. A filter that re-encoded the file changes the size on disk
      // without touching this field, so the two disagreeing is the signature of a corrupted copy.
      final long declaredLength = ByteBuffer.wrap(font, 8, 4).order(ByteOrder.BIG_ENDIAN).getInt() & 0xFFFFFFFFL;
      assertThat(declaredLength)
          .as("%s declares a total length its actual size does not match: the copy is corrupt, which happens "
              + "when resource filtering is applied to a binary", resource)
          .isEqualTo(font.length);
    }
  }

  /**
   * The favicon is the other committed binary Studio serves, and {@code ico} was not on the non-filtered list
   * either. Despite the extension the file is a PNG, which browsers accept - so it is checked as one.
   * <p>
   * The PNG signature is the ideal canary for this particular corruption: its eight bytes deliberately include
   * {@code 0x89} and {@code 0x1A}, chosen by the format precisely to be destroyed by any transfer that treats
   * the file as text.
   */
  @Test
  void theFaviconIsAWellFormedImage() throws IOException {
    final byte[] icon = read("static/favicon.ico");

    assertThat(icon).as("favicon.ico is a PNG in spite of its name, and must keep the signature that says so")
        .startsWith((byte) 0x89, (byte) 'P', (byte) 'N', (byte) 'G', (byte) 0x0D, (byte) 0x0A, (byte) 0x1A,
            (byte) 0x0A);
    assertThat(new String(icon, icon.length - 8, 4, StandardCharsets.US_ASCII))
        .as("a PNG ends with its IEND chunk; a truncated or re-encoded copy does not").isEqualTo("IEND");
  }

  /**
   * The list above is the test: if webpack stops copying a font, or starts copying one this does not know
   * about, the check silently narrows. Asserted rather than assumed, because a test that verifies nothing is
   * worse than no test.
   */
  @Test
  void theWebfontListIsNotEmptyAndEveryEntryResolves() {
    assertThat(WEBFONTS).isNotEmpty();
    for (final String resource : WEBFONTS)
      assertThat(getClass().getClassLoader().getResource(resource))
          .as("%s is missing from the build output entirely", resource).isNotNull();
  }

  private byte[] read(final String resource) throws IOException {
    try (final InputStream in = getClass().getClassLoader().getResourceAsStream(resource)) {
      assertThat(in).as("%s must be on the classpath: the Studio jar serves it to the browser", resource)
          .isNotNull();
      return in.readAllBytes();
    }
  }
}
