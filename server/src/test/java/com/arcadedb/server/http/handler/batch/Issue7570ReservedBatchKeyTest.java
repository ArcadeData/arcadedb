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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7570: the {@code /batch} encoding carries its control keys and its data in the same flat object, and the
 * parsers used to treat "not one of the five control keys" and "is data" as the same predicate. A key the parser did
 * not understand therefore became a property with that name, and the load answered 200.
 * <p>
 * The reported case is the one a reader of the gRPC sibling arrives at: {@code GraphBatchRecord} carries a
 * {@code properties} map, so nesting the properties under a {@code properties} key is the obvious guess. It was
 * accepted, and created a single property literally named {@code properties} holding the whole map - right counters,
 * wrong data, and nothing failing until someone queried for a field that was never stored.
 * <p>
 * These tests pin the two shapes the parsers now refuse: an {@code @}-prefixed key outside the five they understand,
 * and a {@code properties} key whose value is an object. They are unit tests on the parsers themselves;
 * {@code Issue7570BatchReservedKeyIT} drives the same payloads through the HTTP endpoint to prove the refusal reaches
 * the client as a 400.
 */
class Issue7570ReservedBatchKeyTest {

  /**
   * The reported payload. Before the fix this parsed into one property named {@code properties} holding a
   * {@link Map}.
   */
  @Test
  void jsonlRefusesPropertiesNestedUnderAPropertiesKeyOnAVertex() {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"properties\":{\"name\":\"Alice\"}}\n";

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'properties'")
        .hasMessageContaining("line 1")
        .hasMessageContaining("flat");
  }

  /** Same misreading on an edge line: the map is just as wrong there, and just as quiet. */
  @Test
  void jsonlRefusesPropertiesNestedUnderAPropertiesKeyOnAnEdge() {
    final String input = "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\","
        + "\"properties\":{\"since\":2020}}\n";

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'properties'");
  }

  /**
   * The refusal is keyed on the value being an object, because that is what identifies the nested-form mistake. A
   * scalar under the same name is ordinary data - a domain is allowed a field called {@code properties} - and
   * refusing it would break payloads that are not mistakes.
   */
  @Test
  void jsonlKeepsAScalarPropertiesValueAsAnOrdinaryProperty() throws Exception {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"properties\":\"public\"}\n";

    final BatchRecord record = first(input);

    assertThat(record.propertyCount).isEqualTo(1);
    assertThat(record.properties[0]).isEqualTo("properties");
    assertThat(record.properties[1]).isEqualTo("public");
  }

  /**
   * An unrecognised control key. {@code @rid} is what a client that round-trips a record it read back sends, and it
   * used to be stored as a property whose name starts with {@code @}.
   */
  @Test
  void jsonlRefusesAnUnknownAtPrefixedKey() {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"@rid\":\"#1:0\"}\n";

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@rid'")
        .hasMessageContaining("line 1")
        .hasMessageContaining("@type");
  }

  /**
   * A typo in a control key is the same defect wearing a different hat: {@code @clas} used to be accepted as a
   * property and the line then failed on the missing {@code @class} - or, with {@code @class} also present,
   * succeeded and stored the typo.
   */
  @Test
  void jsonlRefusesATypoedControlKey() {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@clas\":\"Person\"}\n";

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@clas'");
  }

  /** The line number has to be the offending one, not the first: a batch failure is placed inside the file. */
  @Test
  void jsonlReportsTheOffendingLineNumber() {
    final String input = """
        {"@type":"vertex","@class":"Person","@id":"p1","name":"Alice"}
        {"@type":"vertex","@class":"Person","@id":"p2","name":"Bob"}
        {"@type":"vertex","@class":"Person","@id":"p3","@cat":"v"}
        """;

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("line 3");
  }

  /**
   * The refusal must not be a {@link MalformedBatchRecordException}: that subclass means "this line is not a JSON
   * object at all", which {@code PostBatchHandler} reports as a 408 when the body also ended early, because a
   * truncated upload produces exactly that. A reserved key is a well-formed line the server declines, and has to
   * stay a 400.
   */
  @Test
  void aReservedKeyIsNotReportedAsAMalformedLine() {
    final String input = "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@rid\":\"#1:0\"}\n";

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(MalformedBatchRecordException.class);
  }

  /** The five control keys and ordinary data still parse exactly as before. */
  @Test
  void jsonlStillAcceptsTheFiveControlKeysAndFlatProperties() throws Exception {
    final BatchRecord vertex = first("{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"name\":\"Alice\"}\n");
    assertThat(vertex.kind).isEqualTo(BatchRecord.Kind.VERTEX);
    assertThat(vertex.tempId).isEqualTo("p1");
    assertThat(vertex.propertyCount).isEqualTo(1);
    assertThat(vertex.properties[0]).isEqualTo("name");

    final BatchRecord edge = first(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"since\":2020}\n");
    assertThat(edge.kind).isEqualTo(BatchRecord.Kind.EDGE);
    assertThat(edge.fromRef).isEqualTo("p1");
    assertThat(edge.toRef).isEqualTo("p2");
    assertThat(edge.propertyCount).isEqualTo(1);
    assertThat(edge.properties[0]).isEqualTo("since");
  }

  /**
   * An embedded document under any other name is still legitimate - issue #4069 made nested objects unwrap to a
   * {@link Map} on purpose - so the refusal must be about the key, not about the value being an object.
   */
  @Test
  void jsonlStillAcceptsANestedObjectUnderAnyOtherName() throws Exception {
    final BatchRecord record = first(
        "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"address\":{\"city\":\"Rome\"}}\n");

    assertThat(record.propertyCount).isEqualTo(1);
    assertThat(record.properties[0]).isEqualTo("address");
    assertThat(record.properties[1]).isInstanceOf(Map.class);
  }

  /**
   * CSV names its control keys in the header, so that is where the same defect lives: an unrecognised
   * {@code @}-prefixed column used to become a property column.
   */
  @Test
  void csvRefusesAnUnknownAtPrefixedHeaderColumn() {
    final String input = """
        @type,@class,@id,@rid,name
        vertex,Person,p1,#1:0,Alice
        """;

    assertThatThrownBy(() -> drainCsv(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@rid'")
        .hasMessageContaining("line 1")
        .hasMessageContaining("@type");
  }

  /**
   * The edge section gets its own header, so it gets its own check - and the line number reported has to be the
   * second header's, not the first's.
   */
  @Test
  void csvRefusesAnUnknownAtPrefixedColumnInTheEdgeSectionHeader() {
    final String input = """
        @type,@class,@id,name
        vertex,Person,p1,Alice
        vertex,Person,p2,Bob
        ---
        @type,@class,@from,@to,@weight
        edge,Knows,p1,p2,3
        """;

    assertThatThrownBy(() -> drainCsv(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@weight'")
        .hasMessageContaining("line 5");
  }

  /**
   * CSV values are scalars - {@code parseValue} returns only Boolean, Long, Double, String or null, never a Map - so
   * the nested-form misreading has no CSV spelling and a {@code properties} column is ordinary data. Refusing it
   * would break payloads that are not mistakes.
   */
  @Test
  void csvKeepsAPropertiesColumnAsAnOrdinaryProperty() throws Exception {
    final String input = """
        @type,@class,@id,properties
        vertex,Person,p1,public
        """;

    final BatchRecord record = firstCsv(input);

    assertThat(record.propertyCount).isEqualTo(1);
    assertThat(record.properties[0]).isEqualTo("properties");
    assertThat(record.properties[1]).isEqualTo("public");
  }

  /** The documented CSV shape still parses. */
  @Test
  void csvStillAcceptsTheDocumentedHeaderShape() throws Exception {
    final String input = """
        @type,@class,@id,name
        vertex,Person,p1,Alice
        """;

    final BatchRecord record = firstCsv(input);

    assertThat(record.kind).isEqualTo(BatchRecord.Kind.VERTEX);
    assertThat(record.typeName).isEqualTo("Person");
    assertThat(record.tempId).isEqualTo("p1");
    assertThat(record.propertyCount).isEqualTo(1);
    assertThat(record.properties[0]).isEqualTo("name");
  }

  private static void drain(final String input) throws Exception {
    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      while (stream.hasNext())
        stream.next();
    }
  }

  private static BatchRecord first(final String input) throws Exception {
    try (final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      return stream.next();
    }
  }

  private static void drainCsv(final String input) throws Exception {
    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      while (stream.hasNext())
        stream.next();
    }
  }

  private static BatchRecord firstCsv(final String input) throws Exception {
    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      return stream.next();
    }
  }

  private static ByteArrayInputStream toStream(final String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }
}
