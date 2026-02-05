/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.query.opencypher.functions.convert.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for OpenCypher convert functions.
 */
class OpenCypherConvertFunctionsTest {

  // ============ ConvertToInteger tests ============

  @Test
  void convertToIntegerFromNumber() {
    final ConvertToInteger fn = new ConvertToInteger();
    assertThat(fn.getName()).isEqualTo("convert.toInteger");

    assertThat(fn.execute(new Object[]{42}, null)).isEqualTo(42L);
    assertThat(fn.execute(new Object[]{42L}, null)).isEqualTo(42L);
    assertThat(fn.execute(new Object[]{42.9}, null)).isEqualTo(42L); // Truncates
    assertThat(fn.execute(new Object[]{42.1f}, null)).isEqualTo(42L);
  }

  @Test
  void convertToIntegerFromBoolean() {
    final ConvertToInteger fn = new ConvertToInteger();

    assertThat(fn.execute(new Object[]{true}, null)).isEqualTo(1L);
    assertThat(fn.execute(new Object[]{false}, null)).isEqualTo(0L);
  }

  @Test
  void convertToIntegerFromString() {
    final ConvertToInteger fn = new ConvertToInteger();

    assertThat(fn.execute(new Object[]{"42"}, null)).isEqualTo(42L);
    assertThat(fn.execute(new Object[]{"  42  "}, null)).isEqualTo(42L); // Trimmed
    assertThat(fn.execute(new Object[]{"42.9"}, null)).isEqualTo(42L); // Parsed as double, then truncated
    assertThat(fn.execute(new Object[]{"invalid"}, null)).isNull();
  }

  @Test
  void convertToIntegerNullHandling() {
    final ConvertToInteger fn = new ConvertToInteger();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ ConvertToFloat tests ============

  @Test
  void convertToFloatFromNumber() {
    final ConvertToFloat fn = new ConvertToFloat();
    assertThat(fn.getName()).isEqualTo("convert.toFloat");

    assertThat((Double) fn.execute(new Object[]{42}, null)).isCloseTo(42.0, within(0.001));
    assertThat((Double) fn.execute(new Object[]{42.5}, null)).isCloseTo(42.5, within(0.001));
  }

  @Test
  void convertToFloatFromString() {
    final ConvertToFloat fn = new ConvertToFloat();

    assertThat((Double) fn.execute(new Object[]{"42.5"}, null)).isCloseTo(42.5, within(0.001));
    assertThat((Double) fn.execute(new Object[]{"  42.5  "}, null)).isCloseTo(42.5, within(0.001));
    assertThat(fn.execute(new Object[]{"invalid"}, null)).isNull();
  }

  @Test
  void convertToFloatNullHandling() {
    final ConvertToFloat fn = new ConvertToFloat();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ ConvertToBoolean tests ============

  @Test
  void convertToBooleanFromBoolean() {
    final ConvertToBoolean fn = new ConvertToBoolean();
    assertThat(fn.getName()).isEqualTo("convert.toBoolean");

    assertThat(fn.execute(new Object[]{true}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{false}, null)).isEqualTo(false);
  }

  @Test
  void convertToBooleanFromString() {
    final ConvertToBoolean fn = new ConvertToBoolean();

    assertThat(fn.execute(new Object[]{"true"}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{"TRUE"}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{"false"}, null)).isEqualTo(false);
    assertThat(fn.execute(new Object[]{"FALSE"}, null)).isEqualTo(false);
    assertThat(fn.execute(new Object[]{"yes"}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{"no"}, null)).isEqualTo(false);
  }

  @Test
  void convertToBooleanFromNumber() {
    final ConvertToBoolean fn = new ConvertToBoolean();

    assertThat(fn.execute(new Object[]{1}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{0}, null)).isEqualTo(false);
    assertThat(fn.execute(new Object[]{42}, null)).isEqualTo(true); // Non-zero is true
  }

  @Test
  void convertToBooleanNullHandling() {
    final ConvertToBoolean fn = new ConvertToBoolean();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ ConvertToJson tests ============

  @Test
  void convertToJsonFromPrimitives() {
    final ConvertToJson fn = new ConvertToJson();
    assertThat(fn.getName()).isEqualTo("convert.toJson");

    // Primitives are returned as their string representation
    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{42}, null)).isEqualTo("42");
    assertThat(fn.execute(new Object[]{true}, null)).isEqualTo("true");
    assertThat(fn.execute(new Object[]{null}, null)).isEqualTo("null");
  }

  @Test
  void convertToJsonFromMap() {
    final ConvertToJson fn = new ConvertToJson();

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", "Alice");
    map.put("age", 30);

    final String result = (String) fn.execute(new Object[]{map}, null);
    assertThat(result).contains("\"name\"");
    assertThat(result).contains("\"Alice\"");
    assertThat(result).contains("30");
  }

  @Test
  void convertToJsonFromList() {
    final ConvertToJson fn = new ConvertToJson();

    final List<Object> list = Arrays.asList(1, 2, 3);
    final String result = (String) fn.execute(new Object[]{list}, null);
    assertThat(result).isEqualTo("[1,2,3]");
  }

  // ============ ConvertFromJsonMap tests ============

  @Test
  void convertFromJsonMapBasic() {
    final ConvertFromJsonMap fn = new ConvertFromJsonMap();
    assertThat(fn.getName()).isEqualTo("convert.fromJsonMap");

    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{"{\"name\": \"Alice\", \"age\": 30}"}, null);

    assertThat(result).containsEntry("name", "Alice");
    assertThat(result).containsEntry("age", 30);
  }

  @Test
  void convertFromJsonMapNullHandling() {
    final ConvertFromJsonMap fn = new ConvertFromJsonMap();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ ConvertFromJsonList tests ============

  @Test
  void convertFromJsonListBasic() {
    final ConvertFromJsonList fn = new ConvertFromJsonList();
    assertThat(fn.getName()).isEqualTo("convert.fromJsonList");

    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{"[1, 2, 3]"}, null);
    assertThat(result).containsExactly(1, 2, 3);
  }

  @Test
  void convertFromJsonListNullHandling() {
    final ConvertFromJsonList fn = new ConvertFromJsonList();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ ConvertToList tests ============

  @Test
  void convertToListFromCollection() {
    final ConvertToList fn = new ConvertToList();
    assertThat(fn.getName()).isEqualTo("convert.toList");

    // From List - returns a copy
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{Arrays.asList(1, 2, 3)}, null);
    assertThat(result).containsExactly(1, 2, 3);
  }

  @Test
  void convertToListFromSet() {
    final ConvertToList fn = new ConvertToList();

    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{Set.of(1, 2, 3)}, null);
    assertThat(result).hasSize(3);
    assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  void convertToListFromSingleValue() {
    final ConvertToList fn = new ConvertToList();

    // Single value is wrapped in a list
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{"single"}, null);
    assertThat(result).containsExactly("single");
  }

  @Test
  void convertToListFromArrayWrapsAsElement() {
    final ConvertToList fn = new ConvertToList();

    // Object[] is treated as a single value (not a collection), so it gets wrapped
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{new Object[]{1, 2, 3}}, null);
    // The array is wrapped as a single element
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isInstanceOf(Object[].class);
  }

  @Test
  void convertToListNullHandling() {
    final ConvertToList fn = new ConvertToList();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ ConvertToSet tests ============

  @Test
  void convertToSetFromListRemovesDuplicates() {
    final ConvertToSet fn = new ConvertToSet();
    assertThat(fn.getName()).isEqualTo("convert.toSet");

    // Note: ConvertToSet returns a List (deduped), not a Set
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{Arrays.asList(1, 2, 2, 3, 3, 3)}, null);
    assertThat(result).hasSize(3);
    assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  void convertToSetNullHandling() {
    final ConvertToSet fn = new ConvertToSet();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void convertToSetRequiresCollection() {
    final ConvertToSet fn = new ConvertToSet();

    assertThatThrownBy(() -> fn.execute(new Object[]{"not a collection"}, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires a list or collection");
  }

  // ============ ConvertToMap tests ============

  @Test
  void convertToMapFromMap() {
    final ConvertToMap fn = new ConvertToMap();
    assertThat(fn.getName()).isEqualTo("convert.toMap");

    final Map<String, Object> input = new HashMap<>();
    input.put("a", 1);
    input.put("b", 2);

    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(new Object[]{input}, null);
    assertThat(result).containsEntry("a", 1);
    assertThat(result).containsEntry("b", 2);
  }

  @Test
  void convertToMapNullHandling() {
    final ConvertToMap fn = new ConvertToMap();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void convertToMapInvalidInput() {
    final ConvertToMap fn = new ConvertToMap();

    assertThatThrownBy(() -> fn.execute(new Object[]{"not a map"}, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot convert");
  }

  // ============ Metadata tests ============

  @Test
  void convertFunctionsMetadata() {
    final ConvertToInteger intFn = new ConvertToInteger();
    assertThat(intFn.getMinArgs()).isEqualTo(1);
    assertThat(intFn.getMaxArgs()).isEqualTo(1);
    assertThat(intFn.getDescription()).isNotEmpty();

    final ConvertToJson jsonFn = new ConvertToJson();
    assertThat(jsonFn.getMinArgs()).isEqualTo(1);
    assertThat(jsonFn.getMaxArgs()).isEqualTo(1);
    assertThat(jsonFn.getDescription()).contains("JSON");
  }
}
