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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreateVertexStatementExecutionTest extends TestHelper {
  public CreateVertexStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  public void testVerticesContentJsonArray() {
    final String className = "testVertexContentArray";
    database.getSchema().buildVertexType().withName(className).withTotalBuckets(1).create();

    String array = IntStream.range(0, 1000).mapToObj(i -> String.format("{'name':'name%d', 'surname':'surname%d'}", i, i))
        .collect(Collectors.joining(",", "[", "]"));
    ResultSet result = database.command("sql", "create vertex " + className + " content " + array);

    for (int i = 0; i < 1000; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name" + i);
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname" + i);
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);

    for (int i = 0; i < 1000; i++) {
      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name" + i);
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname" + i);
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testInsertSet() {
    final String className = "testInsertSet";
    final Schema schema = database.getSchema();
    schema.createVertexType(className);

    ResultSet result = database.command("sql", "create vertex " + className + " set name = 'name1'");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testInsertSetNoVertex() {
    final String className = "testInsertSetNoVertex";
    final Schema schema = database.getSchema();
    schema.createDocumentType(className);

    try {
      final ResultSet result = database.command("sql", "create vertex " + className + " set name = 'name1'");
      fail("");
    } catch (final CommandExecutionException e1) {
    } catch (final Exception e2) {
      fail("");
    }
  }

  @Test
  public void testInsertValue() {
    final String className = "testInsertValue";
    final Schema schema = database.getSchema();
    schema.createVertexType(className);

    ResultSet result = database.command("sql", "create vertex " + className + "  (name, surname) values ('name1', 'surname1')");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testInsertValue2() {
    final String className = "testInsertValue2";
    final Schema schema = database.getSchema();
    schema.createVertexType(className);

    ResultSet result = database.command("sql",
        "create vertex " + className + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");

    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name" + (i + 1));
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname" + (i + 1));
    }
    assertThat(result.hasNext()).isFalse();

    final Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      names.remove(item.<String>getProperty("name"));
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(names.isEmpty()).isTrue();
    result.close();
  }

  @Test
  public void testContent() {
    final String className = "testContent";
    final Schema schema = database.getSchema();
    schema.createVertexType(className);

    ResultSet result = database.command("sql", "create vertex " + className + " content {'name':'name1', 'surname':'surname1'}");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
