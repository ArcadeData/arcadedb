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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7574: the other half of the {@code /batch} control-key problem. Issue #7570 refused an {@code @} key the
 * parsers do <b>not</b> understand; a key they <b>do</b> understand, sent on the kind of line that cannot use it,
 * was silently dropped.
 * <p>
 * {@code JsonlBatchRecordStream.parseLine} read {@code @id} only on a vertex and {@code @from}/{@code @to} only on
 * an edge, and then skipped every control key when it collected the properties - so the value was neither stored
 * nor reported and the load answered 200. {@code CsvBatchRecordStream} reached the same outcome by a different
 * route: it skipped the five control columns unconditionally.
 * <p>
 * The client this bites is the one that models both line shapes with one struct, which the gRPC sibling
 * {@code GraphBatchRecord} invites since it carries {@code temp_id} for both kinds. Such a client emits
 * {@code "@id"} on its edges and gets a load that looks clean.
 * <p>
 * The rule is the same in both encodings and is keyed on a value being <b>present</b>: a key carrying nothing -
 * JSON null, the empty string, an empty CSV field - is ignored, which is what keeps the single-CSV-header form
 * working. {@code Issue7574BatchMisplacedControlKeyIT} drives the same payloads through the HTTP endpoint.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7574">issue #7574</a>
 */
class Issue7574MisplacedBatchControlKeyTest {

  /** The payload from the issue. Before the fix this parsed cleanly and the {@code @id} was gone. */
  @Test
  void jsonlRefusesAnIdOnAnEdgeLine() {
    final String input = "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":\"e1\"}\n";

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@id'")
        .hasMessageContaining("line 1")
        .as("the message has to say which kind of line the key belongs to, or the caller is left guessing "
            + "whether the key or the line is what is wrong")
        .hasMessageContaining("vertex line");
  }

  @Test
  void jsonlRefusesFromOrToOnAVertexLine() {
    assertThatThrownBy(() -> drain("{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"@from\":\"x\"}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@from'")
        .hasMessageContaining("edge line");

    assertThatThrownBy(() -> drain("{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"@to\":\"y\"}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@to'")
        .hasMessageContaining("edge line");
  }

  /**
   * The key is not turned into a property either, which is the outcome the #7570 rule would have produced had
   * the misplaced key simply been removed from the understood set. A property literally named {@code @id} is the
   * data corruption #7570 exists to prevent, so the two rules must not be made to fight.
   */
  @Test
  void aMisplacedControlKeyIsRefusedRatherThanStoredAsAProperty() {
    assertThatThrownBy(() -> drain("{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":\"e1\"}\n"))
        .hasMessageContaining("Control key");
  }

  /**
   * A refusal, not a malformed line. {@code MalformedBatchRecordException} means "this line is not a JSON object
   * at all", which {@code PostBatchHandler} reports as a 408 when the body also ended early - the signature of a
   * truncated upload. A misplaced key is a well-formed line the server declines, and has to stay a 400.
   */
  @Test
  void aMisplacedControlKeyIsNotReportedAsAMalformedLine() {
    assertThatThrownBy(() -> drain("{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":\"e1\"}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(MalformedBatchRecordException.class);
  }

  /** The line number reported has to be the offending one, not the first. */
  @Test
  void jsonlReportsTheOffendingLineNumber() {
    final String input = """
        {"@type":"vertex","@class":"Person","@id":"p1","name":"Alice"}
        {"@type":"vertex","@class":"Person","@id":"p2","name":"Bob"}
        {"@type":"edge","@class":"Knows","@from":"p1","@to":"p2","@id":"e1"}
        """;

    assertThatThrownBy(() -> drain(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("line 3");
  }

  /**
   * A key that carries nothing is not a value the load would have dropped, so it is not refused. This is the
   * JSONL half of the rule that keeps the shared-shape client working: it may leave the inapplicable member in
   * its struct as long as it leaves it unset.
   */
  @Test
  void jsonlIgnoresAControlKeyThatCarriesNothing() throws Exception {
    final BatchRecord nullId = first(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":null}\n");
    assertThat(nullId.kind).isEqualTo(BatchRecord.Kind.EDGE);
    assertThat(nullId.propertyCount).as("and it does not become a property either").isZero();

    final BatchRecord emptyId = first(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":\"\"}\n");
    assertThat(emptyId.kind).isEqualTo(BatchRecord.Kind.EDGE);

    final BatchRecord emptyFrom = first(
        "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"@from\":\"\",\"@to\":null}\n");
    assertThat(emptyFrom.tempId).isEqualTo("p1");
  }

  /**
   * A control key carrying a NON-STRING value. The empty-elision above tests {@code instanceof String}, so a
   * number or a boolean skips it and reaches the refusal - which is the behaviour we want and the branch a
   * later edit could most easily invert by loosening the test to "is falsy". Pinned because it is an easy case
   * to get wrong and nothing else covers it (claude-review on PR #7749).
   */
  @Test
  void jsonlRefusesAMisplacedControlKeyCarryingANumberOrABoolean() {
    assertThatThrownBy(() -> drain(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":7}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@id'")
        .hasMessageContaining("vertex line");

    assertThatThrownBy(() -> drain(
        "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"@from\":false}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@from'")
        .hasMessageContaining("edge line");

    // Zero and false are values a client SENT, not values it left unset: only the two JSON spellings of
    // "carries nothing" are ignored, and neither of these is one of them.
    assertThatThrownBy(() -> drain(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@id\":0}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@id'");
  }

  /**
   * Whitespace is a value too. The elision is {@code isEmpty()}, not {@code isBlank()}, so a single space is
   * refused - a client that sent one meant to send something, and silently dropping it is the defect this issue
   * is about.
   */
  @Test
  void jsonlRefusesAMisplacedControlKeyCarryingWhitespace() {
    assertThatThrownBy(() -> drain(
        "{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"@from\":\" \"}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@from'");
  }

  /**
   * The '@' prefix rule of #7570 still owns a MIS-CASED control key: '@ID' is not '@id', so it is refused as an
   * unknown control key rather than reaching the misplacement check. Pinned so a future case-insensitive match
   * cannot be added to one rule without the other noticing.
   */
  @Test
  void aMisCasedControlKeyIsStillAnUnknownControlKeyNotAMisplacedOne() {
    assertThatThrownBy(() -> drain(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"@ID\":\"e1\"}\n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown control key")
        .hasMessageContaining("'@ID'");
  }

  /** Nothing about the well-formed payloads changes. */
  @Test
  void jsonlStillAcceptsEachControlKeyOnItsOwnKindOfLine() throws Exception {
    final BatchRecord vertex = first("{\"@type\":\"vertex\",\"@class\":\"Person\",\"@id\":\"p1\",\"name\":\"Alice\"}\n");
    assertThat(vertex.tempId).isEqualTo("p1");
    assertThat(vertex.propertyCount).isEqualTo(1);

    final BatchRecord edge = first(
        "{\"@type\":\"edge\",\"@class\":\"Knows\",\"@from\":\"p1\",\"@to\":\"p2\",\"since\":2020}\n");
    assertThat(edge.fromRef).isEqualTo("p1");
    assertThat(edge.toRef).isEqualTo("p2");
    assertThat(edge.propertyCount).isEqualTo(1);
  }

  /** CSV reaches the same drop by a different route, so it gets the same refusal. */
  @Test
  void csvRefusesANonEmptyControlValueTheRowsKindCannotUse() {
    final String idOnAnEdge = """
        @type,@class,@id,@from,@to,since
        edge,Knows,e1,p1,p2,2020
        """;

    assertThatThrownBy(() -> drainCsv(idOnAnEdge))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@id'")
        .hasMessageContaining("line 2")
        .hasMessageContaining("vertex line");

    final String fromOnAVertex = """
        @type,@class,@id,@from,@to,name
        vertex,Person,p1,someone,,Alice
        """;

    assertThatThrownBy(() -> drainCsv(fromOnAVertex))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'@from'")
        .hasMessageContaining("edge line");
  }

  /**
   * The CSV rule is keyed on the VALUE, not on the column, and this is the payload that forces it. The documented
   * CSV shape puts a fresh header after the {@code ---} sentinel, but one header carrying all five control columns
   * across both sections works today and is a reasonable thing for a client to emit. Refusing the column would
   * break it; refusing only a value that was actually sent leaves it working.
   */
  @Test
  void csvKeepsAcceptingOneHeaderAcrossBothSectionsWhenTheUnusedColumnsAreEmpty() throws Exception {
    final String input = """
        @type,@class,@id,@from,@to,name
        vertex,Person,p1,,,Alice
        vertex,Person,p2,,,Bob
        edge,Knows,,p1,p2,
        """;

    try (final CsvBatchRecordStream stream = new CsvBatchRecordStream(toStream(input))) {
      assertThat(stream.hasNext()).isTrue();
      final BatchRecord firstVertex = stream.next();
      assertThat(firstVertex.kind).isEqualTo(BatchRecord.Kind.VERTEX);
      assertThat(firstVertex.tempId).isEqualTo("p1");

      assertThat(stream.hasNext()).isTrue();
      stream.next();

      assertThat(stream.hasNext()).isTrue();
      final BatchRecord edge = stream.next();
      assertThat(edge.kind).isEqualTo(BatchRecord.Kind.EDGE);
      assertThat(edge.fromRef).isEqualTo("p1");
      assertThat(edge.toRef).isEqualTo("p2");
      assertThat(edge.tempId).as("an empty '@id' column leaves the edge without one, it does not fabricate a value")
          .isNull();
    }
  }

  /** And the documented two-section shape is untouched. */
  @Test
  void csvStillAcceptsTheDocumentedTwoSectionShape() {
    final String input = """
        @type,@class,@id,name
        vertex,Person,p1,Alice
        vertex,Person,p2,Bob
        ---
        @type,@class,@from,@to,since
        edge,Knows,p1,p2,2020
        """;

    assertThatCode(() -> drainCsv(input)).doesNotThrowAnyException();
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

  private static InputStream toStream(final String input) {
    return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
  }
}
