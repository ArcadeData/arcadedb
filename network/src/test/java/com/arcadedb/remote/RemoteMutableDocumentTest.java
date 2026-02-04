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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteMutableDocumentTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteDocumentType mockType;
  private RemoteMutableDocument document;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    mockType = mock(RemoteDocumentType.class);

    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestDoc")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("TestDoc");

    document = new RemoteMutableDocument(mockDatabase, "TestDoc");
  }

  @Test
  void getTypeThrowsUnsupported() {
    assertThatThrownBy(() -> document.getType())
        .isInstanceOf(UnsupportedOperationException.class);
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
  void setBufferThrowsUnsupported() {
    assertThatThrownBy(() -> document.setBuffer(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void checkForLazyLoadingPropertiesDoesNothing() {
    // Should not throw
    document.checkForLazyLoadingProperties();
  }

  @Test
  void convertValueToSchemaTypeReturnsValueUnchanged() {
    final Object value = "testValue";
    final Object result = document.convertValueToSchemaType("prop", value, null);
    assertThat(result).isSameAs(value);
  }

  @Test
  void convertValueToSchemaTypeWithIntegerReturnsUnchanged() {
    final Object value = 42;
    final Object result = document.convertValueToSchemaType("prop", value, null);
    assertThat(result).isSameAs(value);
  }

  @Test
  void getTypeName() {
    assertThat(document.getTypeName()).isEqualTo("TestDoc");
  }

  @Test
  void toMapWithoutMetadata() {
    document.set("name", "test");
    final var map = document.toMap(false);
    assertThat(map).containsEntry("name", "test");
    assertThat(map).doesNotContainKey("@cat");
  }

  @Test
  void toMapWithMetadata() {
    document.set("name", "test");
    final var map = document.toMap(true);
    assertThat(map).containsEntry("name", "test");
    assertThat(map).containsEntry("@cat", "d");
    assertThat(map).containsEntry("@type", "TestDoc");
    // No RID set, so @rid should not be present
    assertThat(map).doesNotContainKey("@rid");
  }
}
