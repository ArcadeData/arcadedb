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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class RemotePropertyTest {

  private DocumentType mockOwner;
  private RemoteProperty property;

  @BeforeEach
  void setUp() {
    mockOwner = mock(DocumentType.class);

    final Map<String, Object> record = new HashMap<>();
    record.put("name", "testProperty");
    record.put("type", "STRING");
    record.put("id", 1);

    property = new RemoteProperty(mockOwner, record);
  }

  @Test
  void constructorSetsBasicProperties() {
    assertThat(property.getName()).isEqualTo("testProperty");
    assertThat(property.getType()).isEqualTo(Type.STRING);
    assertThat(property.getId()).isEqualTo(1);
  }

  @Test
  void reloadWithOfType() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("ofType", "INTEGER");

    property.reload(entry);

    assertThat(property.getOfType()).isEqualTo("INTEGER");
  }

  @Test
  void reloadWithMandatory() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("mandatory", true);

    property.reload(entry);

    assertThat(property.isMandatory()).isTrue();
  }

  @Test
  void reloadWithReadOnly() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("readOnly", true);

    property.reload(entry);

    assertThat(property.isReadonly()).isTrue();
  }

  @Test
  void reloadWithNotNull() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("notNull", true);

    property.reload(entry);

    assertThat(property.isNotNull()).isTrue();
  }

  @Test
  void reloadWithMin() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("min", "0");

    property.reload(entry);

    assertThat(property.getMin()).isEqualTo("0");
  }

  @Test
  void reloadWithMax() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("max", "100");

    property.reload(entry);

    assertThat(property.getMax()).isEqualTo("100");
  }

  @Test
  void reloadWithHidden() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("hidden", true);

    property.reload(entry);

    assertThat(property.isHidden()).isTrue();
  }

  @Test
  void reloadWithDefault() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("default", 42); // Use a non-String default to avoid expression parsing

    property.reload(entry);

    // Use reflection to verify the value was set without triggering expression parsing
    assertThat(property).extracting("defaultValue").isEqualTo(42);
  }

  @Test
  void reloadWithRegexp() {
    final Map<String, Object> entry = new HashMap<>();
    entry.put("regexp", "^[a-z]+$");

    property.reload(entry);

    assertThat(property.getRegexp()).isEqualTo("^[a-z]+$");
  }

  @Test
  void reloadWithCustom() {
    final Map<String, Object> customValues = new HashMap<>();
    customValues.put("key1", "value1");
    customValues.put("key2", 42);

    final Map<String, Object> entry = new HashMap<>();
    entry.put("custom", customValues);

    property.reload(entry);

    assertThat(property.getCustomValue("key1")).isEqualTo("value1");
    assertThat(property.getCustomValue("key2")).isEqualTo(42);
    assertThat(property.getCustomKeys()).contains("key1", "key2");
  }

  @Test
  void reloadWithAllProperties() {
    final Map<String, Object> customValues = new HashMap<>();
    customValues.put("customKey", "customValue");

    final Map<String, Object> entry = new HashMap<>();
    entry.put("ofType", "LONG");
    entry.put("mandatory", true);
    entry.put("readOnly", true);
    entry.put("notNull", true);
    entry.put("min", "1");
    entry.put("max", "999");
    entry.put("hidden", true);
    entry.put("default", 10); // Use non-String to avoid expression parsing
    entry.put("regexp", "\\d+");
    entry.put("custom", customValues);

    property.reload(entry);

    assertThat(property.getOfType()).isEqualTo("LONG");
    assertThat(property.isMandatory()).isTrue();
    assertThat(property.isReadonly()).isTrue();
    assertThat(property.isNotNull()).isTrue();
    assertThat(property.getMin()).isEqualTo("1");
    assertThat(property.getMax()).isEqualTo("999");
    assertThat(property.isHidden()).isTrue();
    assertThat(property).extracting("defaultValue").isEqualTo(10);
    assertThat(property.getRegexp()).isEqualTo("\\d+");
    assertThat(property.getCustomValue("customKey")).isEqualTo("customValue");
  }

  @Test
  void setDefaultValueThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setDefaultValue("value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setOfTypeThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setOfType("INTEGER"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setReadonlyThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setReadonly(true))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setMandatoryThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setMandatory(true))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setNotNullThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setNotNull(true))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setHiddenThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setHidden(true))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setMaxThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setMax("100"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setMinThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setMin("0"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setRegexpThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setRegexp(".*"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void setCustomValueThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> property.setCustomValue("key", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
