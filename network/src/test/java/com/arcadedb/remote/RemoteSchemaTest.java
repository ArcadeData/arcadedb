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

import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class RemoteSchemaTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema   schema;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    schema = new RemoteSchema(mockDatabase);
  }

  @Test
  void getEmbeddedReturnsNull() {
    assertThat(schema.getEmbedded()).isNull();
  }

  // UnsupportedOperationException tests for builders

  @Test
  void buildTypeIndexThrowsUnsupported() {
    assertThatThrownBy(() -> schema.buildTypeIndex("Type", new String[]{"prop"}))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void buildBucketIndexThrowsUnsupported() {
    assertThatThrownBy(() -> schema.buildBucketIndex("Type", "bucket", new String[]{"prop"}))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void buildManualIndexThrowsUnsupported() {
    assertThatThrownBy(() -> schema.buildManualIndex("idx", new Type[]{Type.STRING}))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void buildDocumentTypeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.buildDocumentType())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void buildVertexTypeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.buildVertexType())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void buildEdgeTypeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.buildEdgeType())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // Deprecated create methods with List<Bucket>

  @Test
  void createDocumentTypeWithBucketsListThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createDocumentType("Type", (List<Bucket>) null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createDocumentTypeWithBucketsAndPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createDocumentType("Type", 1, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createDocumentTypeWithBucketsListAndPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createDocumentType("Type", (List<Bucket>) null, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateDocumentTypeWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getOrCreateDocumentType("Type", 1, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createEdgeTypeWithBucketsListThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createEdgeType("Type", (List<Bucket>) null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createVertexTypeWithBucketsListThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createVertexType("Type", (List<Bucket>) null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createVertexTypeWithBucketsAndPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createVertexType("Type", 1, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createVertexTypeWithBucketsListAndPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createVertexType("Type", (List<Bucket>) null, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateVertexTypeWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() ->
        schema.buildVertexType().withName("Type").withTotalBuckets(1).withIgnoreIfExists(true).withPageSize(100).create())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createEdgeTypeWithBucketsAndPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createEdgeType("Type", 1, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createEdgeTypeWithBucketsListAndPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createEdgeType("Type", (List<Bucket>) null, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateEdgeTypeWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getOrCreateEdgeType("Type", 1, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // Timezone/encoding methods

  @Test
  void getTimeZoneThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getTimeZone())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setTimeZoneThrowsUnsupported() {
    assertThatThrownBy(() -> schema.setTimeZone(TimeZone.getDefault()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getZoneIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getZoneId())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setZoneIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.setZoneId(ZoneId.systemDefault()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getDateFormatThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getDateFormat())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setDateFormatThrowsUnsupported() {
    assertThatThrownBy(() -> schema.setDateFormat("yyyy-MM-dd"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getDateTimeFormatThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getDateTimeFormat())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setDateTimeFormatThrowsUnsupported() {
    assertThatThrownBy(() -> schema.setDateTimeFormat("yyyy-MM-dd HH:mm:ss"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getEncodingThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getEncoding())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setEncodingThrowsUnsupported() {
    assertThatThrownBy(() -> schema.setEncoding("UTF-8"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // Function methods

  @Test
  void registerFunctionLibraryThrowsUnsupported() {
    assertThatThrownBy(() -> schema.registerFunctionLibrary(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void unregisterFunctionLibraryThrowsUnsupported() {
    assertThatThrownBy(() -> schema.unregisterFunctionLibrary("lib"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getFunctionLibrariesThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getFunctionLibraries())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void hasFunctionLibraryThrowsUnsupported() {
    assertThatThrownBy(() -> schema.hasFunctionLibrary("lib"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getFunctionLibraryThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getFunctionLibrary("lib"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getFunctionThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getFunction("lib", "func"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // File/bucket/index methods

  @Test
  void getFileByIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getFileById(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getFileByIdIfExistsThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getFileByIdIfExists(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBucketByIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getBucketById(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void copyTypeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.copyType("t", "nt", null, 1, 100, 1000))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getIndexesThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getIndexes())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getIndexByNameThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getIndexByName("idx"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTypeIndexWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createTypeIndex(null, true, "Type", new String[]{"p"}, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTypeIndexWithCallbackThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createTypeIndex(null, true, "Type", new String[]{"p"}, 100, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTypeIndexWithNullStrategyThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createTypeIndex(null, true, "Type", new String[]{"p"}, 100, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateTypeIndexWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getOrCreateTypeIndex(null, true, "Type", new String[]{"p"}, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateTypeIndexWithCallbackThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getOrCreateTypeIndex(null, true, "Type", new String[]{"p"}, 100, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateTypeIndexWithNullStrategyThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getOrCreateTypeIndex(null, true, "Type", new String[]{"p"}, 100, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createBucketIndexThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createBucketIndex(null, true, "Type", "bucket", new String[]{"p"}, 100, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createManualIndexThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createManualIndex(null, true, "idx", new Type[]{Type.STRING}, 100, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getTypeNameByBucketIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getTypeNameByBucketId(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getTypeByBucketIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getTypeByBucketId(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getInvolvedTypeByBucketIdThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getInvolvedTypeByBucketId(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getDictionaryThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getDictionary())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // Trigger methods

  @Test
  void getTriggerThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getTrigger("t"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getTriggersThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getTriggers())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getTriggersForTypeThrowsUnsupported() {
    assertThatThrownBy(() -> schema.getTriggersForType("Type"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTriggerThrowsUnsupported() {
    assertThatThrownBy(() -> schema.createTrigger(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
