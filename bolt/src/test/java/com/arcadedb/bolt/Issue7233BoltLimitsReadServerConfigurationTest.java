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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.packstream.PackStreamReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7233: the BOLT protocol limits are SCOPE.SERVER, so they live in the server's {@link ContextConfiguration}
 * - written by the server configuration file, {@code SET SERVER SETTING} and the MCP tool - and used to be read off
 * the {@link GlobalConfiguration} enum, which only a system property or an environment variable ever writes. A
 * cluster that tightened one of these in its configuration file ran on the compiled-in default instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7233BoltLimitsReadServerConfigurationTest {

  @Test
  void theReassembledMessageBoundComesFromTheServerConfiguration() throws IOException {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE, 8);

    final BoltChunkedInput input = new BoltChunkedInput(new ByteArrayInputStream(chunkedMessage(32)), configuration);

    assertThatThrownBy(input::readMessage).isInstanceOf(IOException.class)
        .hasMessageContaining("exceeds 8 bytes");
  }

  @Test
  void thePackStreamValueBoundComesFromTheServerConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"bolt.packstream.maxValueLength\":4}}");

    // A 16-character string: the marker says 16, the configured ceiling says 4.
    final byte[] data = new byte[18];
    data[0] = (byte) 0xD0; // STRING_8
    data[1] = 16;
    final PackStreamReader reader = new PackStreamReader(data, configuration);

    assertThatThrownBy(reader::readValue).isInstanceOf(IOException.class)
        .hasMessageContaining("exceeds the maximum allowed (4)");
  }

  /** One BOLT chunk of {@code size} zero bytes, followed by the end-of-message marker. */
  private static byte[] chunkedMessage(final int size) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write((size >> 8) & 0xFF);
    out.write(size & 0xFF);
    out.write(new byte[size]);
    out.write(0);
    out.write(0);
    return out.toByteArray();
  }
}
