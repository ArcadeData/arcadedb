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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CsvBatchRecordStreamTest {

  @Test
  void parseVerticesAndEdges() throws Exception {
    final String input = """
        @type,@class,@id,name,age
        vertex,Person,t1,Alice,30
        vertex,Person,t2,Bob,25
        ---
        @type,@class,@from,@to,since
        edge,KNOWS,t1,t2,2020
        """;

    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      // First vertex
      assertThat(stream.hasNext()).isTrue();
      BatchRecord rec = stream.next();
      assertThat(rec.kind).isEqualTo(BatchRecord.Kind.VERTEX);
      assertThat(rec.typeName).isEqualTo("Person");
      assertThat(rec.tempId).isEqualTo("t1");
      assertThat(rec.propertyCount).isEqualTo(2);
      assertThat(rec.properties[0]).isEqualTo("name");
      assertThat(rec.properties[1]).isEqualTo("Alice");
      assertThat(rec.properties[2]).isEqualTo("age");
      assertThat(rec.properties[3]).isEqualTo(30L); // parsed as Long

      // Second vertex
      assertThat(stream.hasNext()).isTrue();
      rec = stream.next();
      assertThat(rec.tempId).isEqualTo("t2");
      assertThat(rec.properties[1]).isEqualTo("Bob");

      // Edge
      assertThat(stream.hasNext()).isTrue();
      rec = stream.next();
      assertThat(rec.kind).isEqualTo(BatchRecord.Kind.EDGE);
      assertThat(rec.typeName).isEqualTo("KNOWS");
      assertThat(rec.fromRef).isEqualTo("t1");
      assertThat(rec.toRef).isEqualTo("t2");
      assertThat(rec.propertyCount).isEqualTo(1);
      assertThat(rec.properties[0]).isEqualTo("since");
      assertThat(rec.properties[1]).isEqualTo(2020L);

      assertThat(stream.hasNext()).isFalse();
    }
  }

  @Test
  void quotedFields() throws Exception {
    final String input = "@type,@class,@id,name,description\n"
        + "vertex,Person,t1,\"Alice, Jr.\",\"She said \"\"hello\"\"\"\n";

    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord rec = stream.next();
      assertThat(rec.propertyCount).isEqualTo(2);
      assertThat(rec.properties[1]).isEqualTo("Alice, Jr.");
      assertThat(rec.properties[3]).isEqualTo("She said \"hello\"");
    }
  }

  @Test
  void emptyFieldsSkipped() throws Exception {
    final String input = """
        @type,@class,@id,name,age
        vertex,Person,t1,Alice,
        """;

    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord rec = stream.next();
      // Only 'name' should be a property; empty 'age' is skipped
      assertThat(rec.propertyCount).isEqualTo(1);
      assertThat(rec.properties[0]).isEqualTo("name");
    }
  }

  @Test
  void missingTypeColumnThrows() {
    final String input = """
        @class,@id,name
        Person,t1,Alice
        """;

    assertThatThrownBy(() -> {
      try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
        stream.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("@type");
  }

  @Test
  void parseValueTypes() {
    assertThat(CsvBatchRecordStream.parseValue("42")).isEqualTo(42L);
    assertThat(CsvBatchRecordStream.parseValue("3.14")).isEqualTo(3.14);
    assertThat(CsvBatchRecordStream.parseValue("true")).isEqualTo(true);
    assertThat(CsvBatchRecordStream.parseValue("false")).isEqualTo(false);
    assertThat(CsvBatchRecordStream.parseValue("hello")).isEqualTo("hello");
    assertThat(CsvBatchRecordStream.parseValue("-10")).isEqualTo(-10L);
    assertThat(CsvBatchRecordStream.parseValue("1.5e2")).isEqualTo(150.0);
  }

  @Test
  void parseCsvFieldsBasic() {
    assertThat(CsvBatchRecordStream.parseCsvFields("a,b,c")).containsExactly("a", "b", "c");
    assertThat(CsvBatchRecordStream.parseCsvFields("\"a,b\",c")).containsExactly("a,b", "c");
    assertThat(CsvBatchRecordStream.parseCsvFields("a,,c")).containsExactly("a", "", "c");
  }

  @Test
  void edgeWithExistingRid() throws Exception {
    final String input = """
        @type,@class,@from,@to,weight
        edge,KNOWS,#5:0,#5:1,0.8
        """;

    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord rec = stream.next();
      assertThat(rec.fromRef).isEqualTo("#5:0");
      assertThat(rec.toRef).isEqualTo("#5:1");
      assertThat(rec.properties[1]).isEqualTo(0.8);
    }
  }

  private static ByteArrayInputStream toStream(final String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }
}
