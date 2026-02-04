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
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteMutableVertexTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteVertexType mockType;
  private RemoteMutableVertex vertex;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    mockType = mock(RemoteVertexType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestVertex")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("TestVertex");

    vertex = new RemoteMutableVertex(mockDatabase, "TestVertex");
  }

  @Test
  void getTypeThrowsUnsupported() {
    assertThatThrownBy(() -> vertex.getType())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getDatabaseThrowsUnsupported() {
    assertThatThrownBy(() -> vertex.getDatabase())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBufferThrowsUnsupported() {
    assertThatThrownBy(() -> vertex.getBuffer())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setBufferThrowsUnsupported() {
    assertThatThrownBy(() -> vertex.setBuffer(null))
        .isInstanceOf(UnsupportedOperationException.class);
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
  void convertValueToSchemaTypeReturnsValueUnchanged() {
    final Object value = "testValue";
    final Object result = vertex.convertValueToSchemaType("prop", value, null);
    assertThat(result).isSameAs(value);
  }

  @Test
  void getRemoteDatabase() {
    assertThat(vertex.getRemoteDatabase()).isEqualTo(mockDatabase);
  }

  @Test
  void checkForLazyLoadingPropertiesDoesNothing() {
    // Should not throw
    vertex.checkForLazyLoadingProperties();
  }

  @Test
  void getTypeName() {
    assertThat(vertex.getTypeName()).isEqualTo("TestVertex");
  }

  @Test
  void propertiesAsMapReturnsNewMap() {
    vertex.set("key1", "value1");
    final var map = vertex.propertiesAsMap();
    assertThat(map).containsEntry("key1", "value1");
    // Verify it's a copy
    map.put("key2", "value2");
    assertThat(vertex.has("key2")).isFalse();
  }

  @Test
  void toMapWithoutMetadata() {
    vertex.set("name", "test");
    final var map = vertex.toMap(false);
    assertThat(map).containsEntry("name", "test");
    assertThat(map).doesNotContainKey("@cat");
  }

  @Test
  void toMapWithMetadata() {
    vertex.set("name", "test");
    final var map = vertex.toMap(true);
    assertThat(map).containsEntry("name", "test");
    assertThat(map).containsEntry("@cat", "v");
    assertThat(map).containsEntry("@type", "TestVertex");
  }
}
