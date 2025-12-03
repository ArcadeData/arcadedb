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
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

}
