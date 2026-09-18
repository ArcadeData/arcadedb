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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExpandStepTest extends TestHelper {

  @Test
  void shouldExpandCollection() {
    database.getSchema().createDocumentType("Container");

    database.transaction(() -> {
      final List<Integer> values = new ArrayList<>();
      values.add(1);
      values.add(2);
      values.add(3);
      database.newDocument("Container").set("name", "test").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Container");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandNestedDocuments() {
    database.getSchema().createDocumentType("Parent");
    database.getSchema().createDocumentType("Child");

    database.transaction(() -> {
      final MutableDocument child1 = database.newDocument("Child").set("name", "child1");
      final MutableDocument child2 = database.newDocument("Child").set("name", "child2");
      child1.save();
      child2.save();

      final List<Object> children = new ArrayList<>();
      children.add(child1);
      children.add(child2);

      database.newDocument("Parent").set("name", "parent").set("children", children).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(children) FROM Parent");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object name = item.getProperty("name");
      assertThat(name).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void shouldExpandEmptyCollection() {
    database.getSchema().createDocumentType("EmptyContainer");

    database.transaction(() -> {
      final List<Integer> emptyList = new ArrayList<>();
      database.newDocument("EmptyContainer").set("values", emptyList).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM EmptyContainer");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void shouldExpandMultipleCollections() {
    database.getSchema().createDocumentType("MultiContainer");

    database.transaction(() -> {
      final List<Integer> values1 = new ArrayList<>();
      values1.add(1);
      values1.add(2);
      database.newDocument("MultiContainer").set("values", values1).save();

      final List<Integer> values2 = new ArrayList<>();
      values2.add(3);
      values2.add(4);
      database.newDocument("MultiContainer").set("values", values2).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM MultiContainer");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandWithWhereClause() {
    database.getSchema().createDocumentType("FilteredContainer");

    database.transaction(() -> {
      final List<Integer> values1 = new ArrayList<>();
      values1.add(10);
      values1.add(20);
      database.newDocument("FilteredContainer").set("name", "A").set("values", values1).save();

      final List<Integer> values2 = new ArrayList<>();
      values2.add(30);
      database.newDocument("FilteredContainer").set("name", "B").set("values", values2).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM FilteredContainer WHERE name = 'A'");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandSingleValue() {
    database.getSchema().createDocumentType("Single");

    database.transaction(() -> {
      final List<String> values = new ArrayList<>();
      values.add("only");
      database.newDocument("Single").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Single");

    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void shouldExpandLargeCollection() {
    database.getSchema().createDocumentType("Large");

    database.transaction(() -> {
      final List<Integer> values = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        values.add(i);
      }
      database.newDocument("Large").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Large");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandStrings() {
    database.getSchema().createDocumentType("Strings");

    database.transaction(() -> {
      final List<String> values = new ArrayList<>();
      values.add("alpha");
      values.add("beta");
      values.add("gamma");
      database.newDocument("Strings").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Strings");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isGreaterThan(0);
    result.close();
  }

  @Test
  void shouldExpandWithLimit() {
    database.getSchema().createDocumentType("Limited");

    database.transaction(() -> {
      final List<Integer> values = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        values.add(i);
      }
      database.newDocument("Limited").set("values", values).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(values) FROM Limited LIMIT 5");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(5);
    result.close();
  }

  @Test
  void shouldExpandNonExistentField() {
    database.getSchema().createDocumentType("NoField");

    database.transaction(() ->
      database.newDocument("NoField").set("name", "test").save());

    final ResultSet result = database.query("sql", "SELECT expand(nonexistent) FROM NoField");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void expandWithAliasUsesAliasAsPropertyName() {
    final ResultSet result = database.query("sql", "SELECT expand([1,2,3,4]) AS test");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.getPropertyNames()).containsExactly("test");
      assertThat(item.getPropertyNames()).doesNotContain("value");
      count++;
    }

    assertThat(count).isEqualTo(4);
    result.close();
  }

  @Test
  void expandWithoutAliasUsesValueAsPropertyName() {
    final ResultSet result = database.query("sql", "SELECT expand([1,2,3,4])");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.getPropertyNames()).containsExactly("value");
      count++;
    }

    assertThat(count).isEqualTo(4);
    result.close();
  }

  @Test
  void expandStringListWithAliasUsesAliasAsPropertyName() {
    database.getSchema().createDocumentType("AliasTest");

    database.transaction(() ->
      database.newDocument("AliasTest").set("tags", List.of("a", "b", "c")).save());

    final ResultSet result = database.query("sql", "SELECT expand(tags) AS tag FROM AliasTest");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.getPropertyNames()).containsExactly("tag");
      assertThat(item.getPropertyNames()).doesNotContain("value");
      count++;
    }

    assertThat(count).isEqualTo(3);
    result.close();
  }

  @Test
  void expandDocumentListWithAliasPreservesDocumentProperties() {
    database.getSchema().createDocumentType("DocParent");
    database.getSchema().createDocumentType("DocChild");

    database.transaction(() -> {
      final MutableDocument c1 = database.newDocument("DocChild").set("x", 1).save();
      final MutableDocument c2 = database.newDocument("DocChild").set("x", 2).save();
      database.newDocument("DocParent").set("children", List.of(c1, c2)).save();
    });

    final ResultSet result = database.query("sql", "SELECT expand(children) AS ignored FROM DocParent");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      assertThat(item.getPropertyNames()).contains("x");
      assertThat(item.getPropertyNames()).doesNotContain("ignored");
      count++;
    }

    assertThat(count).isEqualTo(2);
    result.close();
  }

  /**
   * Regression test for issue #7787: {@code ExpandStep}'s top-level dispatch had no else arm for a value that is
   * neither {@code Identifiable}, {@code Result}, {@code Iterator} nor {@code Iterable}, so a bare scalar silently
   * produced zero rows although the very same value inside a collection expands fine (proven by the collection-based
   * tests above).
   */
  @Test
  void expandOfAScalarProducesOneRow() {
    database.getSchema().createDocumentType("ScalarExpand");

    database.transaction(() -> database.newDocument("ScalarExpand").set("n", 42).save());

    final ResultSet result = database.query("sql", "SELECT expand(n) FROM ScalarExpand");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Integer>getProperty("value")).isEqualTo(42);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void expandOfAScalarWithAliasUsesAliasAsPropertyName() {
    database.getSchema().createDocumentType("ScalarExpandAlias");

    database.transaction(() -> database.newDocument("ScalarExpandAlias").set("n", 42).save());

    final ResultSet result = database.query("sql", "SELECT expand(n) AS renamed FROM ScalarExpandAlias");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.getPropertyNames()).containsExactly("renamed");
    assertThat(item.<Integer>getProperty("renamed")).isEqualTo(42);
    result.close();
  }

  @Test
  void expandOfAMapProducesOneMapBackedRow() {
    database.getSchema().createDocumentType("MapExpand");

    database.transaction(() -> database.newDocument("MapExpand").set("m", java.util.Map.of("a", 1)).save());

    final ResultSet result = database.query("sql", "SELECT expand(m) FROM MapExpand");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Integer>getProperty("a")).isEqualTo(1);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void expandOfALetScalarProducesOneRow() {
    final ResultSet result = database.query("sql", "SELECT expand($x) FROM (SELECT 1) LET $x = 5");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<Integer>getProperty("value")).isEqualTo(5);
    result.close();
  }

  /**
   * Regression test for issue #7910: the #7787 else arm wrapped a native array whole, so {@code expand()} of an
   * {@code ARRAY_OF_FLOATS} property answered ONE row holding the entire array where the same numbers in a
   * {@code LIST} answer one row per element.
   */
  @Test
  void expandOfANativeArrayProducesOneRowPerElement() {
    final DocumentType type = database.getSchema().createDocumentType("FloatArrayExpand");
    type.createProperty("arrf", Type.ARRAY_OF_FLOATS);

    database.transaction(() -> database.newDocument("FloatArrayExpand").set("arrf", new float[] { 1, 2, 3 }).save());

    final List<Object> values = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT expand(arrf) FROM FloatArrayExpand")) {
      while (result.hasNext())
        values.add(result.next().getProperty("value"));
    }

    assertThat(values).containsExactly(1F, 2F, 3F);
  }

  @Test
  void expandOfANativeArrayAgreesWithTheSameValuesInAList() {
    final DocumentType type = database.getSchema().createDocumentType("ArrayVsList");
    type.createProperty("arrf", Type.ARRAY_OF_FLOATS);
    type.createProperty("list", Type.LIST);

    database.transaction(() -> database.newDocument("ArrayVsList")//
        .set("arrf", new float[] { 1, 2, 3 })//
        .set("list", List.of(1F, 2F, 3F)).save());

    final List<Object> fromArray = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT expand(arrf) FROM ArrayVsList")) {
      while (result.hasNext())
        fromArray.add(result.next().getProperty("value"));
    }

    final List<Object> fromList = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT expand(list) FROM ArrayVsList")) {
      while (result.hasNext())
        fromList.add(result.next().getProperty("value"));
    }

    assertThat(fromArray).isEqualTo(fromList);
  }

  /**
   * {@code UnwindStep} has always routed a native array through {@code MultiValue.getMultiValueIterator}: the two
   * steps in the same package must not disagree about what an array is.
   */
  @Test
  void expandOfANativeArrayAgreesWithUnwind() {
    final DocumentType type = database.getSchema().createDocumentType("ArrayVsUnwind");
    type.createProperty("arrl", Type.ARRAY_OF_LONGS);

    database.transaction(() -> database.newDocument("ArrayVsUnwind").set("arrl", new long[] { 7, 8 }).save());

    final List<Object> expanded = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT expand(arrl) FROM ArrayVsUnwind")) {
      while (result.hasNext())
        expanded.add(result.next().getProperty("value"));
    }

    final List<Object> unwound = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT arrl FROM ArrayVsUnwind UNWIND arrl")) {
      while (result.hasNext())
        unwound.add(result.next().getProperty("arrl"));
    }

    assertThat(expanded).isEqualTo(unwound);
    assertThat(expanded).containsExactly(7L, 8L);
  }

  @Test
  void expandOfEveryNativeArrayTypeProducesOneRowPerElement() {
    final DocumentType type = database.getSchema().createDocumentType("AllArrayExpand");
    type.createProperty("arrs", Type.ARRAY_OF_SHORTS);
    type.createProperty("arri", Type.ARRAY_OF_INTEGERS);
    type.createProperty("arrl", Type.ARRAY_OF_LONGS);
    type.createProperty("arrf", Type.ARRAY_OF_FLOATS);
    type.createProperty("arrd", Type.ARRAY_OF_DOUBLES);

    database.transaction(() -> database.newDocument("AllArrayExpand")//
        .set("arrs", new short[] { 1, 2 })//
        .set("arri", new int[] { 1, 2 })//
        .set("arrl", new long[] { 1, 2 })//
        .set("arrf", new float[] { 1, 2 })//
        .set("arrd", new double[] { 1, 2 }).save());

    for (final String property : List.of("arrs", "arri", "arrl", "arrf", "arrd")) {
      int count = 0;
      try (final ResultSet result = database.query("sql", "SELECT expand(" + property + ") FROM AllArrayExpand")) {
        while (result.hasNext()) {
          assertThat(result.next().<Object>getProperty("value")).isInstanceOf(Number.class);
          count++;
        }
      }
      assertThat(count).as(property).isEqualTo(2);
    }
  }

  @Test
  void expandOfANativeArrayWithAliasUsesAliasAsPropertyName() {
    final DocumentType type = database.getSchema().createDocumentType("ArrayExpandAlias");
    type.createProperty("arri", Type.ARRAY_OF_INTEGERS);

    database.transaction(() -> database.newDocument("ArrayExpandAlias").set("arri", new int[] { 5, 6 }).save());

    int count = 0;
    try (final ResultSet result = database.query("sql", "SELECT expand(arri) AS n FROM ArrayExpandAlias")) {
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item.getPropertyNames()).containsExactly("n");
        count++;
      }
    }

    assertThat(count).isEqualTo(2);
  }

  @Test
  void expandOfAnEmptyNativeArrayProducesNoRows() {
    final DocumentType type = database.getSchema().createDocumentType("EmptyArrayExpand");
    type.createProperty("arri", Type.ARRAY_OF_INTEGERS);

    database.transaction(() -> database.newDocument("EmptyArrayExpand").set("arri", new int[0]).save());

    try (final ResultSet result = database.query("sql", "SELECT expand(arri) FROM EmptyArrayExpand")) {
      assertThat(result.hasNext()).isFalse();
    }
  }

  /**
   * A {@code BINARY} property is a {@code byte[]}, i.e. an opaque blob rather than a sequence of values: expanding it
   * one byte per row would turn a megabyte into a million rows. It stays a single value, like any other scalar.
   */
  @Test
  void expandOfABinaryPropertyProducesOneRowHoldingTheWholeBlob() {
    final DocumentType type = database.getSchema().createDocumentType("BinaryExpand");
    type.createProperty("blob", Type.BINARY);

    database.transaction(() -> database.newDocument("BinaryExpand").set("blob", new byte[] { 1, 2, 3 }).save());

    try (final ResultSet result = database.query("sql", "SELECT expand(blob) FROM BinaryExpand")) {
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<byte[]>getProperty("value")).containsExactly(1, 2, 3);
      assertThat(result.hasNext()).isFalse();
    }
  }

  /**
   * {@code expand()} flattens one level only: a list nested in a list yields one row whose value IS the nested list.
   * A nested array must behave the same way, so the array arm must not flatten deeper than the list arm does.
   */
  /**
   * The {@code BINARY} guard belongs to {@code MultiValue.isSequenceArray()}, which {@code UnwindStep} shares, so
   * {@code UNWIND} of a blob must not explode it one byte per row either: the two steps have to agree.
   */
  @Test
  void unwindOfABinaryPropertyProducesOneRowHoldingTheWholeBlob() {
    final DocumentType type = database.getSchema().createDocumentType("BinaryUnwind");
    type.createProperty("blob", Type.BINARY);

    database.transaction(() -> database.newDocument("BinaryUnwind").set("blob", new byte[] { 1, 2, 3 }).save());

    try (final ResultSet result = database.query("sql", "SELECT blob FROM BinaryUnwind UNWIND blob")) {
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<byte[]>getProperty("blob")).containsExactly(1, 2, 3);
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void expandFlattensOneLevelOnlyForArraysJustLikeForLists() {
    final List<Object> fromNestedLists = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT expand($x) FROM (SELECT 1) LET $x = [ [1,2], [3,4] ]")) {
      while (result.hasNext())
        fromNestedLists.add(result.next().getProperty("value"));
    }

    // a list nested in a list is NOT flattened further: one row per nested list, each holding the whole nested list
    assertThat(fromNestedLists).hasSize(2);
    assertThat(fromNestedLists.getFirst()).isInstanceOf(List.class);

    final DocumentType type = database.getSchema().createDocumentType("NestedArrayExpand");
    type.createProperty("outer", Type.LIST);

    database.transaction(() -> database.newDocument("NestedArrayExpand")//
        .set("outer", List.of(new int[] { 1, 2 }, new int[] { 3, 4 })).save());

    int count = 0;
    try (final ResultSet result = database.query("sql", "SELECT expand(outer) FROM NestedArrayExpand")) {
      while (result.hasNext()) {
        // an array nested in a list must behave exactly as the nested list above: one row, not flattened further
        assertThat(result.next().<Object>getProperty("value")).isNotInstanceOf(Number.class);
        count++;
      }
    }

    assertThat(count).isEqualTo(2);
  }
}
