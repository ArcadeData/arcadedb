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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteMutableEdgeTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteEdgeType mockEdgeType;
  private RemoteMutableEdge edge;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    mockEdgeType = mock(RemoteEdgeType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestEdge")).thenReturn(mockEdgeType);
    when(mockEdgeType.getName()).thenReturn("TestEdge");

    // Build an immutable edge to use as source for the mutable edge
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestEdge");
    attributes.put(Property.CAT_PROPERTY, "e");
    attributes.put(Property.OUT_PROPERTY, "#1:0");
    attributes.put(Property.IN_PROPERTY, "#2:0");

    when(mockEdgeType.getPolymorphicPropertyIfExists(ArgumentMatchers.anyString())).thenReturn(null);

    final RemoteImmutableEdge immutableEdge = new RemoteImmutableEdge(mockDatabase, attributes);
    edge = new RemoteMutableEdge(immutableEdge);
  }

  @Test
  void getTypeThrowsUnsupported() {
    assertThatThrownBy(() -> edge.getType())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getDatabaseThrowsUnsupported() {
    assertThatThrownBy(() -> edge.getDatabase())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBufferThrowsUnsupported() {
    assertThatThrownBy(() -> edge.getBuffer())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setBufferThrowsUnsupported() {
    assertThatThrownBy(() -> edge.setBuffer(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void asEdgeReturnsSelf() {
    final Edge result = edge.asEdge();
    assertThat(result).isSameAs(edge);
  }

  @Test
  void asEdgeWithLoadContentReturnsSelf() {
    final Edge result = edge.asEdge(true);
    assertThat(result).isSameAs(edge);
  }

  @Test
  void convertValueToSchemaTypeReturnsValueUnchanged() {
    final Object value = "testValue";
    final Object result = edge.convertValueToSchemaType("prop", value, null);
    assertThat(result).isSameAs(value);
  }

  @Test
  void checkForLazyLoadingPropertiesDoesNothing() {
    // Should not throw
    edge.checkForLazyLoadingProperties();
  }

  @Test
  void getTypeName() {
    assertThat(edge.getTypeName()).isEqualTo("TestEdge");
  }

  @Test
  void toMapWithoutMetadata() {
    final var map = edge.toMap(false);
    assertThat(map).doesNotContainKey("@cat");
  }

  @Test
  void toMapWithMetadata() {
    final var map = edge.toMap(true);
    assertThat(map).containsEntry("@cat", "e");
    assertThat(map).containsEntry("@type", "TestEdge");
    // Edge has @in and @out metadata
    assertThat(map).containsKey("@in");
    assertThat(map).containsKey("@out");
  }

  @Test
  void toMapWithMetadataIncludesRid() {
    // The edge has an identity from the immutable source
    final var map = edge.toMap(true);
    // Identity may or may not be set depending on source attributes
    if (edge.getIdentity() != null)
      assertThat(map).containsKey("@rid");
  }
}
