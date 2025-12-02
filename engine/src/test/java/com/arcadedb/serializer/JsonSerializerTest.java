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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonSerializerTest extends TestHelper {

  private JsonSerializer jsonSerializer;

  @BeforeEach
  void setUp() {
    jsonSerializer = JsonSerializer.createJsonSerializer();
  }

  @Test
  void serializeDocument() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createDocumentType("TestType");
      type.createProperty("key1", Type.STRING);
      type.createProperty("key2", Type.INTEGER);

      MutableDocument document = database.newDocument("TestType")
          .set("key1", "value1")
          .set("key2", 123)
          .save();

      String json = jsonSerializer.serializeDocument(document).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestType");
      assertThat(JsonPath.<String>read(json, "$.key1")).isEqualTo("value1");
      assertThat(JsonPath.<Integer>read(json, "$.key2")).isEqualTo(123);
    });
  }

  @Test
  void serializeVertex() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createVertexType("TestVertexType");
      type.createProperty("key1", Type.STRING);
      type.createProperty("key2", Type.INTEGER);

      MutableVertex vertex = database.newVertex("TestVertexType")
          .set("key1", "value1")
          .set("key2", 123)
          .save();

      String json = jsonSerializer.serializeDocument(vertex).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestVertexType");
      assertThat(JsonPath.<String>read(json, "$.key1")).isEqualTo("value1");
      assertThat(JsonPath.<Integer>read(json, "$.key2")).isEqualTo(123);
    });
  }

  @Test
  void serializeEdge() {
    database.transaction(() -> {
      DocumentType vertexType = database.getSchema().createVertexType("TestVertexType");
      DocumentType edgeType = database.getSchema().createEdgeType("TestEdgeType");

      MutableVertex vertex1 = database.newVertex("TestVertexType").save();
      MutableVertex vertex2 = database.newVertex("TestVertexType").save();

      MutableEdge edge = vertex1.newEdge("TestEdgeType", vertex2).save();

      String json = jsonSerializer.serializeDocument(edge).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestEdgeType");
      assertThat(JsonPath.<String>read(json, "$.@in")).isEqualTo(vertex2.getIdentity().toString());
      assertThat(JsonPath.<String>read(json, "$.@out")).isEqualTo(vertex1.getIdentity().toString());
    });
  }
}
