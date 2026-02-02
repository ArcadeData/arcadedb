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
package com.arcadedb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ContextConfigurationTest {

  private ContextConfiguration config;

  @BeforeEach
  void setUp() {
    config = new ContextConfiguration();
  }

  @Test
  void emptyConstructorCreatesEmptyConfig() {
    assertThat(config.getContextKeys()).isEmpty();
  }

  @Test
  void constructorWithMapCopiesValues() {
    final Map<String, Object> initial = new HashMap<>();
    initial.put("key1", "value1");
    initial.put("key2", 42);

    final ContextConfiguration configWithMap = new ContextConfiguration(initial);

    assertThat(configWithMap.hasValue("key1")).isTrue();
    assertThat(configWithMap.hasValue("key2")).isTrue();
  }

  @Test
  void copyConstructorCopiesValues() {
    config.setValue("testKey", "testValue");

    final ContextConfiguration copy = new ContextConfiguration(config);

    assertThat((String) copy.getValue("testKey", null)).isEqualTo("testValue");
  }

  @Test
  void copyConstructorWithNullDoesNotThrow() {
    final ContextConfiguration copy = new ContextConfiguration((ContextConfiguration) null);
    assertThat(copy.getContextKeys()).isEmpty();
  }

  @Test
  void setValueWithStringName() {
    config.setValue("myKey", "myValue");

    assertThat(config.hasValue("myKey")).isTrue();
    assertThat((String) config.getValue("myKey", null)).isEqualTo("myValue");
  }

  @Test
  void setValueWithNullRemovesKey() {
    config.setValue("myKey", "myValue");
    assertThat(config.hasValue("myKey")).isTrue();

    config.setValue("myKey", null);
    assertThat(config.hasValue("myKey")).isFalse();
  }

  @Test
  void setValueWithGlobalConfiguration() {
    config.setValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION, "java.time.LocalDateTime");

    assertThat((String) config.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION)).isEqualTo("java.time.LocalDateTime");
  }

  @Test
  void setValueWithGlobalConfigurationNullRemovesKey() {
    config.setValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION, "java.time.LocalDateTime");
    config.setValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION, null);

    // Should fall back to global default
    final Object value = config.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION);
    assertThat(value).isEqualTo(GlobalConfiguration.DATE_TIME_IMPLEMENTATION.getDefValue());
  }

  @Test
  void getValueWithDefaultReturnsDefaultWhenNotSet() {
    final String value = config.getValue("nonexistent", "default");
    assertThat(value).isEqualTo("default");
  }

  @Test
  void getValueAsBoolean() {
    config.setValue(GlobalConfiguration.TX_WAL, true);
    assertThat(config.getValueAsBoolean(GlobalConfiguration.TX_WAL)).isTrue();

    config.setValue(GlobalConfiguration.TX_WAL, "false");
    assertThat(config.getValueAsBoolean(GlobalConfiguration.TX_WAL)).isFalse();
  }

  @Test
  void getValueAsBooleanWithNullReturnsFalse() {
    config.setValue(GlobalConfiguration.TX_WAL, null);
    // Should fall back to global default or false
    final boolean value = config.getValueAsBoolean(GlobalConfiguration.TX_WAL);
    // The default value of TX_WAL is true, so we check the type conversion works
    assertThat(value).isEqualTo(GlobalConfiguration.TX_WAL.getValueAsBoolean());
  }

  @Test
  void getValueAsInteger() {
    config.setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, 100);
    assertThat(config.getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(100);

    config.setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, "200");
    assertThat(config.getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(200);
  }

  @Test
  void getValueAsIntegerWithNullReturnsDefault() {
    // Create a new config without the key set
    final ContextConfiguration emptyConfig = new ContextConfiguration();
    // Get a config that has a non-null default
    final int value = emptyConfig.getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
    // Should return the global default
    assertThat(value).isEqualTo(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger());
  }

  @Test
  void getValueAsLong() {
    config.setValue(GlobalConfiguration.FREE_PAGE_RAM, 1000L);
    assertThat(config.getValueAsLong(GlobalConfiguration.FREE_PAGE_RAM)).isEqualTo(1000L);

    config.setValue(GlobalConfiguration.FREE_PAGE_RAM, "2000");
    assertThat(config.getValueAsLong(GlobalConfiguration.FREE_PAGE_RAM)).isEqualTo(2000L);
  }

  @Test
  void getValueAsFloat() {
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 1.5f);
    assertThat(config.getValueAsFloat(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE)).isEqualTo(1.5f);

    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, "2.5");
    assertThat(config.getValueAsFloat(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE)).isEqualTo(2.5f);
  }

  @Test
  void getValueAsString() {
    config.setValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION, "java.time.LocalDateTime");
    assertThat(config.getValueAsString(GlobalConfiguration.DATE_TIME_IMPLEMENTATION)).isEqualTo("java.time.LocalDateTime");
  }

  @Test
  void getValueAsStringWithNameAndDefault() {
    assertThat(config.getValueAsString("nonexistent", "default")).isEqualTo("default");

    config.setValue("existing", "value");
    assertThat(config.getValueAsString("existing", "default")).isEqualTo("value");
  }

  @Test
  void hasValueReturnsTrueWhenSet() {
    assertThat(config.hasValue("myKey")).isFalse();

    config.setValue("myKey", "value");
    assertThat(config.hasValue("myKey")).isTrue();
  }

  @Test
  void getContextKeysReturnsAllKeys() {
    config.setValue("key1", "value1");
    config.setValue("key2", "value2");
    config.setValue("key3", "value3");

    assertThat(config.getContextKeys()).containsExactlyInAnyOrder("key1", "key2", "key3");
  }

  @Test
  void mergeAddsValuesFromOtherConfig() {
    final ContextConfiguration other = new ContextConfiguration();
    other.setValue("otherKey", "otherValue");

    config.setValue("myKey", "myValue");
    config.merge(other);

    assertThat(config.hasValue("myKey")).isTrue();
    assertThat(config.hasValue("otherKey")).isTrue();
    assertThat((String) config.getValue("otherKey", null)).isEqualTo("otherValue");
  }

  @Test
  void mergeOverwritesExistingKeys() {
    final ContextConfiguration other = new ContextConfiguration();
    other.setValue("sharedKey", "newValue");

    config.setValue("sharedKey", "oldValue");
    config.merge(other);

    assertThat((String) config.getValue("sharedKey", null)).isEqualTo("newValue");
  }

  @Test
  void resetClearsAllValues() {
    config.setValue("key1", "value1");
    config.setValue("key2", "value2");

    config.reset();

    assertThat(config.getContextKeys()).isEmpty();
  }

  @Test
  void toJsonAndFromJsonRoundTrip() {
    config.setValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION, "java.time.LocalDateTime");

    final String json = config.toJSON();
    assertThat(json).contains("configuration");

    final ContextConfiguration restored = new ContextConfiguration();
    restored.fromJSON(json);

    assertThat((String) restored.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION))
        .isEqualTo("java.time.LocalDateTime");
  }

  @Test
  void fromJsonWithNullDoesNotThrow() {
    config.fromJSON(null);
    assertThat(config.getContextKeys()).isEmpty();
  }

  @Test
  void toJsonFormatsCorrectly() {
    config.setValue(GlobalConfiguration.TX_WAL, true);

    final String json = config.toJSON();

    assertThat(json).contains("configuration");
    // The key should have the prefix stripped
    assertThat(json).contains("txWAL");
  }

  @Test
  void getValueWithTxRetries() {
    // Test with TX_RETRIES which is a valid config
    config.setValue(GlobalConfiguration.TX_RETRIES, 5);
    assertThat(config.getValueAsInteger(GlobalConfiguration.TX_RETRIES)).isEqualTo(5);
  }

  @Test
  void getValueWithTxRetryDelay() {
    // Test with TX_RETRY_DELAY which is a valid config
    config.setValue(GlobalConfiguration.TX_RETRY_DELAY, 100);
    assertThat(config.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)).isEqualTo(100);
  }
}
