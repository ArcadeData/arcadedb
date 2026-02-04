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
package com.arcadedb.remote;

import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteImmutableVertexTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteVertexType mockType;
  private RemoteImmutableVertex vertex;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    mockType = mock(RemoteVertexType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestVertex")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("TestVertex");
    when(mockType.getPolymorphicPropertyIfExists(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);

    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestVertex");
    attributes.put(Property.CAT_PROPERTY, "v");
    attributes.put("name", "testVertex");
    attributes.put("value", 42);
    attributes.put(Property.RID_PROPERTY, "#3:0");

    vertex = new RemoteImmutableVertex(mockDatabase, attributes);
  }

  @Test
  void asVertexReturnsSelf() {
    final Vertex result = vertex.asVertex();
    assertThat(result).isSameAs(vertex);
  }

  @Test
  void asVertexWithLoadContentReturnsSelf() {
    final Vertex result = vertex.asVertex(true);
    assertThat(result).isSameAs(vertex);
  }

  @Test
  void asVertexWithFalseReturnsSelf() {
    final Vertex result = vertex.asVertex(false);
    assertThat(result).isSameAs(vertex);
  }

  @Test
  void propertiesAsMapReturnsCopy() {
    final Map<String, Object> map = vertex.propertiesAsMap();
    assertThat(map).containsEntry("name", "testVertex");
    assertThat(map).containsEntry("value", 42);
    // Verify it's a copy
    map.put("extra", "data");
    assertThat(vertex.has("extra")).isFalse();
  }

  @Test
  void toMapWithoutMetadata() {
    final Map<String, Object> map = vertex.toMap(false);
    assertThat(map).containsEntry("name", "testVertex");
    assertThat(map).doesNotContainKey("@cat");
    assertThat(map).doesNotContainKey("@type");
  }

  @Test
  void toMapWithMetadata() {
    final Map<String, Object> map = vertex.toMap(true);
    assertThat(map).containsEntry("name", "testVertex");
    assertThat(map).containsEntry("@cat", "v");
    assertThat(map).containsEntry("@type", "TestVertex");
    assertThat(map).containsEntry("@rid", "#3:0");
  }

  @Test
  void modifyReturnsRemoteMutableVertex() {
    assertThat(vertex.modify()).isInstanceOf(RemoteMutableVertex.class);
  }

  @Test
  void getRemoteDatabase() {
    assertThat(vertex.getRemoteDatabase()).isEqualTo(mockDatabase);
  }

  @Test
  void getTypeName() {
    assertThat(vertex.getTypeName()).isEqualTo("TestVertex");
  }

  @Test
  void getIdentity() {
    assertThat(vertex.getIdentity()).isNotNull();
    assertThat(vertex.getIdentity().toString()).isEqualTo("#3:0");
  }
}
