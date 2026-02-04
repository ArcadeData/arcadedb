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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteDocumentTypeTest {

  private RemoteDatabase mockDatabase;
  private RemoteSchema mockSchema;
  private RemoteDocumentType type;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockSchema = mock(RemoteSchema.class);
    when(mockDatabase.getSchema()).thenReturn(mockSchema);

    final Result record = createMockResult("TestType", 42, List.of("bucket1", "bucket2"),
        "round-robin", List.of(), List.of(), Collections.emptyMap());

    type = new RemoteDocumentType(mockDatabase, record);
  }

  private Result createMockResult(final String name, final Object records, final List<String> buckets,
      final String bucketSelectionStrategy, final List<String> parentTypes,
      final List<Map<String, Object>> properties, final Map<String, Object> custom) {
    final Result result = mock(Result.class);
    when(result.getProperty("name")).thenReturn(name);
    when(result.getProperty("records")).thenReturn(records);
    when(result.getProperty("buckets")).thenReturn(buckets);
    when(result.getProperty("bucketSelectionStrategy")).thenReturn(bucketSelectionStrategy);
    when(result.getProperty("parentTypes")).thenReturn(parentTypes);
    when(result.getProperty("properties")).thenReturn(properties);
    when(result.hasProperty("aliases")).thenReturn(false);
    when(result.hasProperty("custom")).thenReturn(!custom.isEmpty());
    when(result.getProperty("custom")).thenReturn(custom);
    return result;
  }

  @Test
  void getName() {
    assertThat(type.getName()).isEqualTo("TestType");
  }

  @Test
  void toStringReturnsName() {
    assertThat(type.toString()).isEqualTo("TestType");
  }

  @Test
  void count() {
    assertThat(type.count()).isEqualTo(42);
  }

  @Test
  void constructorWithLongRecordCount() {
    final Result record = createMockResult("LongType", 100L, List.of("b1"),
        "round-robin", List.of(), List.of(), Collections.emptyMap());

    final RemoteDocumentType longType = new RemoteDocumentType(mockDatabase, record);
    assertThat(longType.count()).isEqualTo(100);
  }

  @Test
  void constructorWithNullRecordCount() {
    final Result record = createMockResult("NullType", null, List.of("b1"),
        "round-robin", List.of(), List.of(), Collections.emptyMap());

    final RemoteDocumentType nullType = new RemoteDocumentType(mockDatabase, record);
    assertThat(nullType.count()).isEqualTo(0);
  }

  @Test
  void equalsWithSameName() {
    final Result record2 = createMockResult("TestType", 0, List.of(),
        "round-robin", List.of(), List.of(), Collections.emptyMap());
    final RemoteDocumentType type2 = new RemoteDocumentType(mockDatabase, record2);

    assertThat(type.equals(type2)).isTrue();
  }

  @Test
  void equalsWithDifferentName() {
    final Result record2 = createMockResult("OtherType", 0, List.of(),
        "round-robin", List.of(), List.of(), Collections.emptyMap());
    final RemoteDocumentType type2 = new RemoteDocumentType(mockDatabase, record2);

    assertThat(type.equals(type2)).isFalse();
  }

  @Test
  void equalsWithNonRemoteDocumentType() {
    assertThat(type.equals("TestType")).isFalse();
  }

  @Test
  void isTheSameAs() {
    final Result record2 = createMockResult("TestType", 0, List.of(),
        "round-robin", List.of(), List.of(), Collections.emptyMap());
    final RemoteDocumentType type2 = new RemoteDocumentType(mockDatabase, record2);

    assertThat(type.isTheSameAs(type2)).isTrue();
  }

  @Test
  void hashCodeUsesName() {
    assertThat(type.hashCode()).isEqualTo("TestType".hashCode());
  }

  @Test
  void getSchema() {
    assertThat(type.getSchema()).isEqualTo(mockSchema);
  }

  @Test
  void existsPropertyReturnsTrueForExistingProperty() {
    final Map<String, Object> prop = new HashMap<>();
    prop.put("name", "myProp");
    prop.put("type", "STRING");
    prop.put("id", 1);

    final Result record = createMockResult("WithProps", 0, List.of(),
        "round-robin", List.of(), List.of(prop), Collections.emptyMap());

    final RemoteDocumentType typeWithProps = new RemoteDocumentType(mockDatabase, record);
    assertThat(typeWithProps.existsProperty("myProp")).isTrue();
  }

  @Test
  void existsPropertyReturnsFalseForMissingProperty() {
    assertThat(type.existsProperty("nonExistent")).isFalse();
  }

  @Test
  void getPropertyIfExistsReturnsNullForMissing() {
    assertThat(type.getPropertyIfExists("nonExistent")).isNull();
  }

  @Test
  void getPropertyNames() {
    final Map<String, Object> prop1 = new HashMap<>();
    prop1.put("name", "prop1");
    prop1.put("type", "STRING");
    prop1.put("id", 1);
    final Map<String, Object> prop2 = new HashMap<>();
    prop2.put("name", "prop2");
    prop2.put("type", "INTEGER");
    prop2.put("id", 2);

    final Result record = createMockResult("WithProps", 0, List.of(),
        "round-robin", List.of(), List.of(prop1, prop2), Collections.emptyMap());

    final RemoteDocumentType typeWithProps = new RemoteDocumentType(mockDatabase, record);
    assertThat(typeWithProps.getPropertyNames()).containsExactlyInAnyOrder("prop1", "prop2");
  }

  @Test
  void getProperties() {
    final Map<String, Object> prop = new HashMap<>();
    prop.put("name", "myProp");
    prop.put("type", "STRING");
    prop.put("id", 1);

    final Result record = createMockResult("WithProps", 0, List.of(),
        "round-robin", List.of(), List.of(prop), Collections.emptyMap());

    final RemoteDocumentType typeWithProps = new RemoteDocumentType(mockDatabase, record);
    assertThat(typeWithProps.getProperties()).hasSize(1);
  }

  @Test
  void hasBucketReturnsTrue() {
    assertThat(type.hasBucket("bucket1")).isTrue();
  }

  @Test
  void hasBucketReturnsFalse() {
    assertThat(type.hasBucket("nonExistent")).isFalse();
  }

  @Test
  void getAliasesDefaultEmpty() {
    assertThat(type.getAliases()).isEmpty();
  }

  @Test
  void reloadWithAliases() {
    final Result record = mock(Result.class);
    when(record.getProperty("records")).thenReturn(0);
    when(record.getProperty("buckets")).thenReturn(List.of());
    when(record.getProperty("bucketSelectionStrategy")).thenReturn("round-robin");
    when(record.getProperty("parentTypes")).thenReturn(List.of());
    when(record.getProperty("properties")).thenReturn(List.of());
    when(record.hasProperty("aliases")).thenReturn(true);
    when(record.getProperty("aliases")).thenReturn(List.of("alias1", "alias2"));
    when(record.hasProperty("custom")).thenReturn(false);

    type.reload(record);

    assertThat(type.getAliases()).containsExactlyInAnyOrder("alias1", "alias2");
  }

  @Test
  void getCustomKeysEmpty() {
    assertThat(type.getCustomKeys()).isEmpty();
  }

  @Test
  void getCustomValueReturnsNull() {
    assertThat(type.getCustomValue("missing")).isNull();
  }

  @Test
  void reloadWithCustomProperties() {
    final Map<String, Object> custom = new HashMap<>();
    custom.put("key1", "value1");

    final Result record = createMockResult("TestType", 0, List.of(),
        "round-robin", List.of(), List.of(), custom);

    type.reload(record);

    assertThat(type.getCustomKeys()).contains("key1");
    assertThat(type.getCustomValue("key1")).isEqualTo("value1");
  }

  @Test
  void getIndexesByPropertiesReturnsEmpty() {
    assertThat(type.getIndexesByProperties(List.of("prop1"))).isEmpty();
  }

  @Test
  void getPolymorphicIndexByPropertiesReturnsNull() {
    assertThat(type.getPolymorphicIndexByProperties("prop1")).isNull();
  }

  @Test
  void getPolymorphicIndexByPropertiesListReturnsNull() {
    assertThat(type.getPolymorphicIndexByProperties(List.of("prop1"))).isNull();
  }

  // UnsupportedOperationException tests

  @Test
  void instanceOfThrowsUnsupported() {
    assertThatThrownBy(() -> type.instanceOf("SomeType"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getSubTypesThrowsUnsupported() {
    assertThatThrownBy(() -> type.getSubTypes())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getEventsThrowsUnsupported() {
    assertThatThrownBy(() -> type.getEvents())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setSuperTypesThrowsUnsupported() {
    assertThatThrownBy(() -> type.setSuperTypes(List.of()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getInvolvedBucketsThrowsUnsupported() {
    assertThatThrownBy(() -> type.getInvolvedBuckets())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBucketIdsThrowsUnsupported() {
    assertThatThrownBy(() -> type.getBucketIds(false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void removeBucketThrowsUnsupported() {
    assertThatThrownBy(() -> type.removeBucket(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBucketIdByRecordThrowsUnsupported() {
    assertThatThrownBy(() -> type.getBucketIdByRecord(null, false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBucketIndexByKeysThrowsUnsupported() {
    assertThatThrownBy(() -> type.getBucketIndexByKeys(null, false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getBucketSelectionStrategyThrowsUnsupported() {
    assertThatThrownBy(() -> type.getBucketSelectionStrategy())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setBucketSelectionStrategyWithObjectThrowsUnsupported() {
    assertThatThrownBy(() -> type.setBucketSelectionStrategy(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setBucketSelectionStrategyWithStringThrowsUnsupported() {
    assertThatThrownBy(() -> type.setBucketSelectionStrategy("round-robin"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getAllIndexesThrowsUnsupported() {
    assertThatThrownBy(() -> type.getAllIndexes(false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getPolymorphicBucketIndexByBucketIdThrowsUnsupported() {
    assertThatThrownBy(() -> type.getPolymorphicBucketIndexByBucketId(0, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getIndexesByPropertiesVarargThrowsUnsupported() {
    assertThatThrownBy(() -> type.getIndexesByProperties("prop1", "prop2"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getFirstBucketIdThrowsUnsupported() {
    assertThatThrownBy(() -> type.getFirstBucketId())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void toJSONThrowsUnsupported() {
    assertThatThrownBy(() -> type.toJSON())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getPolymorphicPropertiesWithDefaultDefinedThrowsUnsupported() {
    assertThatThrownBy(() -> type.getPolymorphicPropertiesWithDefaultDefined())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateTypeIndexWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[]{"prop"}, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateTypeIndexWithCallbackThrowsUnsupported() {
    assertThatThrownBy(() -> type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[]{"prop"}, 100, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getOrCreateTypeIndexWithNullStrategyThrowsUnsupported() {
    assertThatThrownBy(() -> type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[]{"prop"}, 100, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTypeIndexWithPageSizeThrowsUnsupported() {
    assertThatThrownBy(() -> type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[]{"prop"}, 100))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTypeIndexWithCallbackThrowsUnsupported() {
    assertThatThrownBy(() -> type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[]{"prop"}, 100, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTypeIndexWithNullStrategyThrowsUnsupported() {
    assertThatThrownBy(() -> type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, new String[]{"prop"}, 100, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void reloadUpdatesExistingProperties() {
    final Map<String, Object> prop = new HashMap<>();
    prop.put("name", "myProp");
    prop.put("type", "STRING");
    prop.put("id", 1);

    final Result record1 = createMockResult("TestType", 10, List.of(),
        "round-robin", List.of(), List.of(prop), Collections.emptyMap());
    final RemoteDocumentType typeWithProp = new RemoteDocumentType(mockDatabase, record1);

    // Reload with same property name but updated attributes
    final Map<String, Object> updatedProp = new HashMap<>();
    updatedProp.put("name", "myProp");
    updatedProp.put("type", "STRING");
    updatedProp.put("id", 1);
    updatedProp.put("mandatory", true);

    final Result record2 = createMockResult("TestType", 20, List.of(),
        "round-robin", List.of(), List.of(updatedProp), Collections.emptyMap());
    typeWithProp.reload(record2);

    assertThat(typeWithProp.count()).isEqualTo(20);
    assertThat(typeWithProp.existsProperty("myProp")).isTrue();
  }
}
