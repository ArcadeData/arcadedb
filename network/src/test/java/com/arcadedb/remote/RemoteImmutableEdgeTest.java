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

import com.arcadedb.graph.Edge;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteImmutableEdgeTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteEdgeType mockType;
  private RemoteImmutableEdge edge;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    mockType = mock(RemoteEdgeType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestEdge")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("TestEdge");
    when(mockType.getPolymorphicPropertyIfExists(ArgumentMatchers.anyString())).thenReturn(null);

    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestEdge");
    attributes.put(Property.CAT_PROPERTY, "e");
    attributes.put(Property.OUT_PROPERTY, "#1:0");
    attributes.put(Property.IN_PROPERTY, "#2:0");
    attributes.put("weight", 10);
    attributes.put(Property.RID_PROPERTY, "#5:0");

    edge = new RemoteImmutableEdge(mockDatabase, attributes);
  }

  @Test
  void getOut() {
    assertThat(edge.getOut()).isNotNull();
    assertThat(edge.getOut().toString()).isEqualTo("#1:0");
  }

  @Test
  void getIn() {
    assertThat(edge.getIn()).isNotNull();
    assertThat(edge.getIn().toString()).isEqualTo("#2:0");
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
  void asEdgeWithFalseReturnsSelf() {
    final Edge result = edge.asEdge(false);
    assertThat(result).isSameAs(edge);
  }

  @Test
  void toMapWithoutMetadata() {
    final Map<String, Object> map = edge.toMap(false);
    assertThat(map).containsEntry("weight", 10);
    assertThat(map).doesNotContainKey("@cat");
    assertThat(map).doesNotContainKey("@type");
  }

  @Test
  void toMapWithMetadata() {
    final Map<String, Object> map = edge.toMap(true);
    assertThat(map).containsEntry("weight", 10);
    assertThat(map).containsEntry("@cat", "e");
    assertThat(map).containsEntry("@type", "TestEdge");
    assertThat(map).containsEntry("@rid", "#5:0");
  }

  @Test
  void modifyReturnsRemoteMutableEdge() {
    assertThat(edge.modify()).isInstanceOf(RemoteMutableEdge.class);
  }

  @Test
  void getTypeName() {
    assertThat(edge.getTypeName()).isEqualTo("TestEdge");
  }

  @Test
  void getIdentity() {
    assertThat(edge.getIdentity()).isNotNull();
    assertThat(edge.getIdentity().toString()).isEqualTo("#5:0");
  }
}
