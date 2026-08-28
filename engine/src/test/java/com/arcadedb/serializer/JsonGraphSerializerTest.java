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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JsonGraphSerializerTest extends TestHelper {

  private JsonGraphSerializer jsonGraphSerializer;
  private MutableEdge         edge;
  private MutableVertex       vertex1;
  private MutableVertex       vertex2;

  @BeforeEach
  void setUp() {
    jsonGraphSerializer = JsonGraphSerializer.createJsonGraphSerializer();

    database.transaction(() -> {
      DocumentType vertexType = database.getSchema().createVertexType("TestVertexType");
      DocumentType edgeType = database.getSchema().createEdgeType("TestEdgeType");

      vertex1 = database.newVertex("TestVertexType").save();
      vertex2 = database.newVertex("TestVertexType").save();

      edge = vertex1.newEdge("TestEdgeType", vertex2).save();
    });
  }

  @Test
  void serializeEdge() {

    String json = jsonGraphSerializer.serializeGraphElement(edge).toString();

    assertThat(JsonPath.<String>read(json, "$.p.@rid")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.p.@type")).isEqualTo("TestEdgeType");
    assertThat(JsonPath.<String>read(json, "$.p.@cat")).isEqualTo("e");
    assertThat(JsonPath.<String>read(json, "$.p.@in")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.p.@out")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.r")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.t")).isEqualTo("TestEdgeType");
    assertThat(JsonPath.<String>read(json, "$.i")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.o")).isNotEmpty();

  }

  @Test
  void serializeEdgeWithoutMetadata() {
    jsonGraphSerializer.setIncludeMetadata(false);

    String json = jsonGraphSerializer.serializeGraphElement(edge).toString();

    //nometadata
    assertThat(JsonPath.<Map<?, ?>>read(json, "$.p")).isEmpty();

    assertThat(JsonPath.<String>read(json, "$.r")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.t")).isEqualTo("TestEdgeType");
    assertThat(JsonPath.<String>read(json, "$.i")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.o")).isNotEmpty();

  }

  /**
   * Issue #6795 (follow-up on #6455): {@code encodeTemporalForWriteBack}'s write-back encoding used to be gated
   * on {@code value instanceof Temporal}, which the default {@code java.time.LocalDate}/{@code LocalDateTime}
   * implementations satisfy but the supported non-default {@code arcadedb.dateImplementation=java.util.Date} (or
   * {@code Calendar}) does not - the binary serializer hands a DATE property back as a plain {@link Date} or
   * {@link Calendar} there, so the guard never fired, the value fell through to the raw epoch-millis encoding
   * {@link com.arcadedb.serializer.json.JSONObject#put(String, Object)}'s default temporal branch uses, and the
   * re-import decoded that millis number as epoch DAYS - the original #6455 data-loss symptom returns. A real
   * {@link Document}/{@link DocumentType}/{@link Property} chain isn't needed to pin the encoding decision itself,
   * so this drives {@link JsonGraphSerializer#serializeGraphElement(Document)} directly against a minimal mocked
   * DATE property, sidestepping how the engine's storage layer happens to represent {@code dateImplementation}
   * internally.
   */
  @Test
  void dateWrittenAsJavaUtilDateIsEncodedAsEpochDaysForWriteBack() {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.set(2024, Calendar.JANUARY, 15);
    final Date birth = calendar.getTime();

    final Property property = mock(Property.class);
    when(property.getType()).thenReturn(Type.DATE);

    final DocumentType type = mock(DocumentType.class);
    when(type.existsProperty("birth")).thenReturn(true);
    when(type.getProperty("birth")).thenReturn(property);

    final Document document = mock(Document.class);
    when(document.getTypeName()).thenReturn("Person");
    when(document.getType()).thenReturn(type);
    when(document.toMap(false)).thenReturn(Map.of("birth", birth));

    final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer()
        .setIncludeMetadata(false)
        .setPrecisionAwareTemporals(true);

    final Object encoded = serializer.serializeGraphElement(document).getJSONObject("p").get("birth");

    assertThat(encoded).isEqualTo(DateUtils.dateToEpochDays(birth));
  }

  /**
   * Same gap, {@code java.util.Calendar} side - the two implementations {@code arcadedb.dateImplementation}
   * supports besides the default {@code java.time.LocalDate}.
   */
  @Test
  void dateWrittenAsCalendarIsEncodedAsEpochDaysForWriteBack() {
    final Calendar birth = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    birth.clear();
    birth.set(2024, Calendar.JANUARY, 15);

    final Property property = mock(Property.class);
    when(property.getType()).thenReturn(Type.DATE);

    final DocumentType type = mock(DocumentType.class);
    when(type.existsProperty("birth")).thenReturn(true);
    when(type.getProperty("birth")).thenReturn(property);

    final Document document = mock(Document.class);
    when(document.getTypeName()).thenReturn("Person");
    when(document.getType()).thenReturn(type);
    when(document.toMap(false)).thenReturn(Map.of("birth", birth));

    final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer()
        .setIncludeMetadata(false)
        .setPrecisionAwareTemporals(true);

    final Object encoded = serializer.serializeGraphElement(document).getJSONObject("p").get("birth");

    assertThat(encoded).isEqualTo(DateUtils.dateToEpochDays(birth));
  }

  /**
   * Sanity check that the fix did not change the well-covered default path: a {@link LocalDate} value must still
   * take the {@code Temporal} branch and encode identically.
   */
  @Test
  void dateWrittenAsLocalDateStillEncodesAsEpochDaysForWriteBack() {
    final LocalDate birth = LocalDate.of(2024, 1, 15);

    final Property property = mock(Property.class);
    when(property.getType()).thenReturn(Type.DATE);

    final DocumentType type = mock(DocumentType.class);
    when(type.existsProperty("birth")).thenReturn(true);
    when(type.getProperty("birth")).thenReturn(property);

    final Document document = mock(Document.class);
    when(document.getTypeName()).thenReturn("Person");
    when(document.getType()).thenReturn(type);
    when(document.toMap(false)).thenReturn(Map.of("birth", birth));

    final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer()
        .setIncludeMetadata(false)
        .setPrecisionAwareTemporals(true);

    final Object encoded = serializer.serializeGraphElement(document).getJSONObject("p").get("birth");

    assertThat(encoded).isEqualTo(birth.toEpochDay());
  }

}
