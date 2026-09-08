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
package com.arcadedb.serializer;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.exception.SerializationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7247: serializing one element of a primitive array used to catch every exception, log a message with no
 * throwable and throw a {@link SerializationException} with no cause. The element's own {@code toString()} is in
 * the message, but which nested {@code serializeValue} threw - and why - was lost on both paths.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7247ArraySerializationCauseTest {

  @Test
  void anArrayElementFailureKeepsItsCause() throws Exception {
    // A fixed buffer that cannot grow: the first few elements fit, then a write inside the loop fails, which is
    // the only way to reach the catch without a database or a schema.
    final Binary content = new Binary(8);
    content.setAutoResizable(false);

    final BinarySerializer serializer = new BinarySerializer(new ContextConfiguration());

    assertThatThrownBy(() -> serializer.serializeValue(null, content, BinaryTypes.TYPE_LIST, new long[64], false))
        .isInstanceOf(SerializationException.class)
        .hasMessageContaining("Error on serializing array value for element")
        .cause().isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("autoResizable=false");
  }

  @Test
  void aFittingArrayStillSerializes() throws Exception {
    final Binary content = new Binary(1024);

    new BinarySerializer(new ContextConfiguration()).serializeValue(null, content, BinaryTypes.TYPE_LIST,
        new long[] { 1L, 2L, 3L }, false);

    assertThat(content.size()).isGreaterThan(0);
  }
}
