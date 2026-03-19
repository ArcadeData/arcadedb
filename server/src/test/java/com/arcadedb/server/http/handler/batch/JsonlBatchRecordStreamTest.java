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
package com.arcadedb.server.http.handler.batch;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonlBatchRecordStreamTest {

  @Test
  void parseVertexWithProperties() throws Exception {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"t1\",\"name\":\"Alice\",\"age\":30}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord rec = stream.next();

      assertThat(rec.kind).isEqualTo(BatchRecord.Kind.VERTEX);
      assertThat(rec.typeName).isEqualTo("Person");
      assertThat(rec.tempId).isEqualTo("t1");
      assertThat(rec.propertyCount).isEqualTo(2);
      assertThat(rec.properties[0]).isEqualTo("name");
      assertThat(rec.properties[1]).isEqualTo("Alice");
      assertThat(rec.properties[2]).isEqualTo("age");
      assertThat(rec.properties[3]).isEqualTo(30);

      assertThat(stream.hasNext()).isFalse();
    }
  }

  @Test
  void parseEdge() throws Exception {
    final String input = "{\"@type\":\"edge\",\"@class\":\"KNOWS\",\"@from\":\"t1\",\"@to\":\"#5:0\",\"since\":2020}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord rec = stream.next();

      assertThat(rec.kind).isEqualTo(BatchRecord.Kind.EDGE);
      assertThat(rec.typeName).isEqualTo("KNOWS");
      assertThat(rec.fromRef).isEqualTo("t1");
      assertThat(rec.toRef).isEqualTo("#5:0");
      assertThat(rec.propertyCount).isEqualTo(1);
      assertThat(rec.properties[0]).isEqualTo("since");
      assertThat(rec.properties[1]).isEqualTo(2020);

      assertThat(stream.hasNext()).isFalse();
    }
  }

  @Test
  void skipBlankLines() throws Exception {
    final String input = "\n{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"t1\"}\n\n{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"t2\"}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      assertThat(stream.next().tempId).isEqualTo("t1");

      assertThat(stream.hasNext()).isTrue();
      assertThat(stream.next().tempId).isEqualTo("t2");

      assertThat(stream.hasNext()).isFalse();
    }
  }

  @Test
  void shortTypeAliases() throws Exception {
    final String input = "{\"@type\":\"v\",\"@class\":\"Person\",\"@id\":\"t1\"}\n{\"@type\":\"e\",\"@class\":\"KNOWS\",\"@from\":\"t1\",\"@to\":\"t1\"}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      assertThat(stream.next().kind).isEqualTo(BatchRecord.Kind.VERTEX);
      assertThat(stream.hasNext()).isTrue();
      assertThat(stream.next().kind).isEqualTo(BatchRecord.Kind.EDGE);
      assertThat(stream.hasNext()).isFalse();
    }
  }

  @Test
  void vertexWithoutTempId() throws Exception {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"name\":\"Bob\"}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord rec = stream.next();
      assertThat(rec.tempId).isNull();
      assertThat(rec.propertyCount).isEqualTo(1);
    }
  }

  @Test
  void unknownTypeThrows() {
    final String input = "{\"@type\":\"unknown\",\"@class\":\"Foo\"}\n";

    assertThatThrownBy(() -> {
      try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
        stream.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown @type");
  }

  @Test
  void lineNumberTracking() throws Exception {
    final String input = "\n\n{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"t1\"}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      assertThat(stream.getLineNumber()).isEqualTo(3);
    }
  }

  @Test
  void recordIsReused() throws Exception {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"t1\"}\n{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"t2\"}\n";

    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      stream.hasNext();
      final BatchRecord rec1 = stream.next();
      stream.hasNext();
      final BatchRecord rec2 = stream.next();
      // Same object reused
      assertThat(rec1).isSameAs(rec2);
    }
  }

  private static ByteArrayInputStream toStream(final String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }
}
