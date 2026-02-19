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

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteImmutableDocumentTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteDocumentType mockType;
  private RemoteImmutableDocument document;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    mockType = mock(RemoteDocumentType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestDoc")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("TestDoc");
    when(mockType.getPolymorphicPropertyIfExists(ArgumentMatchers.anyString())).thenReturn(null);

    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestDoc");
    attributes.put(Property.CAT_PROPERTY, "d");
    attributes.put("name", "testName");
    attributes.put("age", 25);
    attributes.put(Property.RID_PROPERTY, "#1:0");

    document = new RemoteImmutableDocument(mockDatabase, attributes);
  }

  @Test
  void getTypeName() {
    assertThat(document.getTypeName()).isEqualTo("TestDoc");
  }

  @Test
  void getPropertyNames() {
    final Set<String> names = document.getPropertyNames();
    assertThat(names).containsExactlyInAnyOrder("name", "age");
  }

  @Test
  void has() {
    assertThat(document.has("name")).isTrue();
    assertThat(document.has("nonExistent")).isFalse();
  }

  @Test
  void get() {
    assertThat(document.get("name")).isEqualTo("testName");
    assertThat(document.get("age")).isEqualTo(25);
  }

  @Test
  void getReturnsNullForMissing() {
    assertThat(document.get("nonExistent")).isNull();
  }

  @Test
  void getType() {
    assertThat(document.getType()).isEqualTo(mockType);
  }

  @Test
  void getDatabaseThrowsUnsupported() {
    assertThatThrownBy(() -> document.getDatabase())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBufferThrowsUnsupported() {
    assertThatThrownBy(() -> document.getBuffer())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void reloadThrowsUnsupported() {
    assertThatThrownBy(() -> document.reload())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void checkForLazyLoadingReturnsFalse() {
    assertThat(document.checkForLazyLoading()).isFalse();
  }

  @Test
  void modifyReturnsRemoteMutableDocument() {
    assertThat(document.modify()).isInstanceOf(RemoteMutableDocument.class);
  }

  @Test
  void toMapWithoutMetadata() {
    final Map<String, Object> map = document.toMap(false);
    assertThat(map).containsEntry("name", "testName");
    assertThat(map).containsEntry("age", 25);
    assertThat(map).doesNotContainKey("@cat");
    assertThat(map).doesNotContainKey("@type");
  }

  @Test
  void toMapWithMetadata() {
    final Map<String, Object> map = document.toMap(true);
    assertThat(map).containsEntry("name", "testName");
    assertThat(map).containsEntry("@cat", "d");
    assertThat(map).containsEntry("@type", "TestDoc");
    assertThat(map).containsEntry("@rid", "#1:0");
  }

  @Test
  void getIdentity() {
    assertThat(document.getIdentity()).isNotNull();
    assertThat(document.getIdentity().toString()).isEqualTo("#1:0");
  }

  @Test
  void constructorWithNoRid() {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestDoc");
    attributes.put(Property.CAT_PROPERTY, "d");
    attributes.put("field1", "value1");

    final RemoteImmutableDocument doc = new RemoteImmutableDocument(mockDatabase, attributes);
    assertThat(doc.getIdentity()).isNull();
  }

  @Test
  void toMapWithMetadataAndNoRid() {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestDoc");
    attributes.put(Property.CAT_PROPERTY, "d");
    attributes.put("field1", "value1");

    final RemoteImmutableDocument doc = new RemoteImmutableDocument(mockDatabase, attributes);
    final Map<String, Object> map = doc.toMap(true);
    assertThat(map).containsEntry("@cat", "d");
    assertThat(map).doesNotContainKey("@rid");
  }
}
