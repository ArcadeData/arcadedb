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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional coverage tests for SQL methods, exercised via SQL queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLMethodAdditionalCoverageTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("MethDoc");
    database.transaction(() -> {
      database.command("sql",
          """
          INSERT INTO MethDoc SET name = 'Hello World', idx = 42, amount = 123.456, tags = ['alpha', 'beta', 'gamma'], \
          props = {'key1': 'val1', 'key2': 'val2'}, active = true, empty = ''""");
    });
  }

  // --- String methods ---
  @Test
  void appendMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.append(' world') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello world");
    rs.close();
  }

  @Test
  void indexOfMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world'.indexOf('world') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(6);
    rs.close();
  }

  @Test
  void lastIndexOfMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world hello'.lastIndexOf('hello') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(12);
    rs.close();
  }

  @Test
  void leftMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world'.left(5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
    rs.close();
  }

  @Test
  void rightMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world'.right(5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("world");
    rs.close();
  }

  @Test
  void lengthMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.length() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(5);
    rs.close();
  }

  @Test
  void replaceMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'foo bar'.replace('foo', 'baz') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("baz bar");
    rs.close();
  }

  @Test
  void toUpperCaseMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.toUpperCase() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("HELLO");
    rs.close();
  }

  @Test
  void toLowerCaseMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'HELLO'.toLowerCase() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
    rs.close();
  }

  @Test
  void trimMethod() {
    final ResultSet rs = database.query("sql", "SELECT '  hello  '.trim() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
    rs.close();
  }

  @Test
  void subStringMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world'.subString(6) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("world");
    rs.close();
  }

  @Test
  void subStringMethodWithLength() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world'.subString(0, 5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
    rs.close();
  }

  @Test
  void charAtMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.charAt(1) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("e");
    rs.close();
  }

  @Test
  void splitMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'a,b,c'.split(',') as result");
    assertThat(rs.hasNext()).isTrue();
    final Object result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    rs.close();
  }

  @Test
  void capitalizeMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'hello world'.capitalize() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("Hello World");
    rs.close();
  }

  @Test
  void prefixMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'world'.prefix('hello ') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello world");
    rs.close();
  }

  @Test
  void normalizeMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'café'.normalize('NFD') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void formatMethod() {
    final ResultSet rs = database.query("sql", "SELECT name.format('Name: %s') as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("Name: Hello World");
    rs.close();
  }

  @Test
  void trimPrefixMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'xxxhello'.trimPrefix('xxx') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
    rs.close();
  }

  @Test
  void trimSuffixMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'helloaaa'.trimSuffix('a') as result");
    assertThat(rs.hasNext()).isTrue();
    final String result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    // trimSuffix removes trailing characters matching the pattern
    assertThat(result).startsWith("hello");
    rs.close();
  }

  // --- Conversion methods ---
  @Test
  void asIntegerMethod() {
    final ResultSet rs = database.query("sql", "SELECT '42'.asInteger() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(42);
    rs.close();
  }

  @Test
  void asLongMethod() {
    final ResultSet rs = database.query("sql", "SELECT '1234567890'.asLong() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("result")).isEqualTo(1234567890L);
    rs.close();
  }

  @Test
  void asFloatMethod() {
    final ResultSet rs = database.query("sql", "SELECT '3.14'.asFloat() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Float>getProperty("result")).isEqualTo(3.14f);
    rs.close();
  }

  @Test
  void asDoubleMethod() {
    final ResultSet rs = database.query("sql", "SELECT '3.14159'.asDouble() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Double>getProperty("result")).isEqualTo(3.14159);
    rs.close();
  }

  @Test
  void asDecimalMethod() {
    final ResultSet rs = database.query("sql", "SELECT '99.99'.asDecimal() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void asBooleanMethod() {
    final ResultSet rs = database.query("sql", "SELECT 'true'.asBoolean() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    rs.close();
  }

  @Test
  void asStringMethod() {
    final ResultSet rs = database.query("sql", "SELECT (42).asString() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("42");
    rs.close();
  }

  @Test
  void asShortMethod() {
    final ResultSet rs = database.query("sql", "SELECT '123'.asShort() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Short>getProperty("result")).isEqualTo((short) 123);
    rs.close();
  }

  @Test
  void asByteMethod() {
    final ResultSet rs = database.query("sql", "SELECT '7'.asByte() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Byte>getProperty("result")).isEqualTo((byte) 7);
    rs.close();
  }

  @Test
  void asDateMethod() {
    final ResultSet rs = database.query("sql", "SELECT '2024-01-15'.asDate() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void asDateTimeMethod() {
    final ResultSet rs = database.query("sql", "SELECT '2024-01-15 12:30:00'.asDatetime() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void convertMethod() {
    final ResultSet rs = database.query("sql", "SELECT '42'.convert('INTEGER') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(42);
    rs.close();
  }

  @Test
  void asListMethod() {
    final ResultSet rs = database.query("sql", "SELECT [1, 2, 3].asList() as result");
    assertThat(rs.hasNext()).isTrue();
    final Collection<?> lst = rs.next().getProperty("result");
    assertThat(lst).hasSize(3);
    rs.close();
  }

  @Test
  void asSetMethod() {
    final ResultSet rs = database.query("sql", "SELECT [1, 2, 2, 3].asSet() as result");
    assertThat(rs.hasNext()).isTrue();
    final Collection<?> s = rs.next().getProperty("result");
    assertThat(s).hasSize(3);
    rs.close();
  }

  @Test
  void asMapMethod() {
    final ResultSet rs = database.query("sql", "SELECT props.asMap() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final Object result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    rs.close();
  }

  // --- Hash method ---
  @Test
  void hashMD5Method() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.hash('MD5') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void hashSHA256Method() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.hash('SHA-256') as result");
    assertThat(rs.hasNext()).isTrue();
    final String hash = rs.next().getProperty("result");
    assertThat(hash).isNotNull();
    assertThat(hash).isNotEmpty();
    rs.close();
  }

  // --- ifNull / ifEmpty ---
  @Test
  void ifNullMethod() {
    final ResultSet rs = database.query("sql", "SELECT (null).ifNull('default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("default");
    rs.close();
  }

  @Test
  void ifNullMethodNotNull() {
    final ResultSet rs = database.query("sql", "SELECT 'value'.ifNull('default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("value");
    rs.close();
  }

  @Test
  void ifEmptyMethod() {
    final ResultSet rs = database.query("sql", "SELECT ''.ifEmpty('default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("default");
    rs.close();
  }

  @Test
  void ifEmptyMethodNotEmpty() {
    final ResultSet rs = database.query("sql", "SELECT 'value'.ifEmpty('default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("value");
    rs.close();
  }

  // --- Collection methods ---
  @Test
  void sortMethod() {
    final ResultSet rs = database.query("sql", "SELECT [3, 1, 2].sort() as result");
    assertThat(rs.hasNext()).isTrue();
    final List<Integer> sorted = rs.next().getProperty("result");
    assertThat(sorted).containsExactly(1, 2, 3);
    rs.close();
  }

  @Test
  void keysMethod() {
    final ResultSet rs = database.query("sql", "SELECT props.keys() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final Collection<String> keys = rs.next().getProperty("result");
    assertThat(keys).contains("key1", "key2");
    rs.close();
  }

  @Test
  void valuesMethod() {
    final ResultSet rs = database.query("sql", "SELECT props.values() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final Collection<String> values = rs.next().getProperty("result");
    assertThat(values).contains("val1", "val2");
    rs.close();
  }

  @Test
  void sizeMethod() {
    final ResultSet rs = database.query("sql", "SELECT tags.size() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(3);
    rs.close();
  }

  @Test
  void sizeMethodOnString() {
    final ResultSet rs = database.query("sql", "SELECT 'hello'.size() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(5);
    rs.close();
  }

  @Test
  void joinMethod() {
    final ResultSet rs = database.query("sql", "SELECT tags.join(', ') as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final String joined = rs.next().getProperty("result");
    assertThat(joined).contains("alpha");
    assertThat(joined).contains("beta");
    assertThat(joined).contains("gamma");
    rs.close();
  }

  @Test
  void sortDescMethod() {
    final ResultSet rs = database.query("sql", "SELECT [3, 1, 2].sort(false) as result");
    assertThat(rs.hasNext()).isTrue();
    final List<Integer> sorted = rs.next().getProperty("result");
    assertThat(sorted).containsExactly(3, 2, 1);
    rs.close();
  }

  @Test
  void sortAscMethod() {
    final ResultSet rs = database.query("sql", "SELECT [5, 3, 8, 1].sort('asc') as result");
    assertThat(rs.hasNext()).isTrue();
    final List<Integer> sorted = rs.next().getProperty("result");
    assertThat(sorted).containsExactly(1, 3, 5, 8);
    rs.close();
  }

  // --- Record methods ---
  @Test
  void toJsonMethod() {
    final ResultSet rs = database.query("sql", "SELECT @this.toJSON().asString() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final String json = rs.next().getProperty("result");
    assertThat(json).isNotNull();
    assertThat(json).contains("Hello World");
    rs.close();
  }

  @Test
  void asJsonMethod() {
    final ResultSet rs = database.query("sql", "SELECT @this.asJSON() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void excludeMethod() {
    final ResultSet rs = database.query("sql", "SELECT @this.exclude('idx', 'amount') as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    final Object result = item.getProperty("result");
    assertThat(result).isNotNull();
    rs.close();
  }

  @Test
  void includeMethod() {
    final ResultSet rs = database.query("sql", "SELECT @this.include('name', 'idx') as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void typeMethod() {
    final ResultSet rs = database.query("sql", "SELECT name.type() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isNotNull();
    rs.close();
  }

  @Test
  void javaTypeMethod() {
    final ResultSet rs = database.query("sql", "SELECT name.javaType() as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isNotNull();
    rs.close();
  }

  // --- precision (datetime) ---
  @Test
  void precisionMethod() {
    final ResultSet rs = database.query("sql", "SELECT sysdate().precision('millisecond') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNotNull();
    rs.close();
  }

  // --- Chained methods ---
  @Test
  void chainedMethods() {
    final ResultSet rs = database.query("sql", "SELECT '  Hello World  '.trim().toLowerCase() as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello world");
    rs.close();
  }

  @Test
  void chainedMethodsOnField() {
    final ResultSet rs = database.query("sql", "SELECT name.toLowerCase().left(5) as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("hello");
    rs.close();
  }

  // --- Field method on record ---
  @Test
  void fieldMethod() {
    final ResultSet rs = database.query("sql", "SELECT @this.field('name') as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("Hello World");
    rs.close();
  }

  // --- transform method ---
  @Test
  void transformMethod() {
    final ResultSet rs = database.query("sql",
        "SELECT tags.transform('toUpperCase') as result FROM MethDoc");
    assertThat(rs.hasNext()).isTrue();
    final Collection<?> result = rs.next().getProperty("result");
    assertThat(result).isNotNull();
    rs.close();
  }
}
