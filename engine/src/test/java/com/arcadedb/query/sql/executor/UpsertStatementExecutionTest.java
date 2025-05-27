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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UpsertStatementExecutionTest extends TestHelper {
  private String className;

  public UpsertStatementExecutionTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    final DocumentType clazz = database.getSchema().createDocumentType("UpsertStatementExecutionTest");
    clazz.createProperty("name", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    className = clazz.getName();

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("number", 4L);

      final List<String> tagsList = new ArrayList<>();
      tagsList.add("foo");
      tagsList.add("bar");
      tagsList.add("baz");
      doc.set("tagsList", tagsList);

      final Map<String, String> tagsMap = new HashMap<>();
      tagsMap.put("foo", "foo");
      tagsMap.put("bar", "bar");
      tagsMap.put("baz", "baz");
      doc.set("tagsMap", tagsMap);

      doc.save();
    }
  }

  @Override
  protected void endTest() {
    database.transaction(() -> {
      database.command("sql", "delete from UpsertStatementExecutionTest");
    });
  }

  @Test
  public void testUpsertBucket() {
    final List<Bucket> buckets = database.getSchema().getType(className).getBuckets(false);

    final List<Document> resultSet = database.select().fromType(className).where().property("name").eq().value("name1").documents()
        .toList();

    assertThat(resultSet.size()).isEqualTo(1);
    final Document record = resultSet.get(0);

    final String bucketName = database.getSchema().getBucketById(record.getIdentity().getBucketId()).getName();

    // BY BUCKET NAME
    ResultSet result = database.command("sql",
        "update bucket:" + bucketName + " set foo = 'bar' upsert where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo( 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    // BY BUCKET ID
    result = database.command("sql",
        "update bucket:" + record.getIdentity().getBucketId() + " set foo = 'bar' upsert where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo( 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from bucket:" + buckets.get(0).getName());
    assertThat(result.hasNext()).isTrue();

    while (result.hasNext()) {
      item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      if ("name1".equals(name)) {
        assertThat(item.<String>getProperty("foo")).isEqualTo("bar");
      } else {
        assertThat(item.<String>getProperty("foo")).isNull();
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.command("sql", "update bucket:" + bucketName + " remove foo upsert where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from bucket:" + bucketName);
    assertThat(result.hasNext()).isTrue();

    while (result.hasNext()) {
      item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      assertThat(item.<String>getProperty("foo")).isNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testUpsert1() {
    ResultSet result = database.command("sql", "update " + className + " set foo = 'bar' upsert where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      if ("name1".equals(name)) {
        assertThat(item.<String>getProperty("foo")).isEqualTo("bar");
      } else {
        assertThat(item.<String>getProperty("foo")).isNull();
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/380
   */
  @Test
  public void testUpsertVertices() {
    database.command("sql", "CREATE vertex TYPE extra_node");
    database.command("sql", "CREATE PROPERTY extra_node.extraitem STRING");
    database.command("sql", "CREATE INDEX ON extra_node (extraitem) UNIQUE");
    final ResultSet result = database.command("sql",
        "update extra_node set extraitem = 'Hugo2' upsert return after $current where extraitem = 'Hugo'");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final Vertex current = item.getProperty("$current");
    assertThat(current.getString("extraitem")).isEqualTo("Hugo2");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testUpsertAndReturn() {
    final ResultSet result = database.command("sql",
        "update " + className + " set foo = 'bar' upsert  return after  where name = 'name1' ");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("foo")).isEqualTo("bar");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testUpsert2() {

    ResultSet result = database.command("sql", "update " + className + " set foo = 'bar' upsert where name = 'name11'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 11; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      if ("name11".equals(name)) {
        assertThat(item.<String>getProperty("foo")).isEqualTo("bar");
      } else {
        assertThat(item.<String>getProperty("foo")).isNull();
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testUpsertVertices2() {
    final VertexType clazz = database.getSchema().createVertexType("UpsertableVertex");
    clazz.createProperty("name", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newVertex("UpsertableVertex");
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("number", 4L);

      final List<String> tagsList = new ArrayList<>();
      tagsList.add("foo");
      tagsList.add("bar");
      tagsList.add("baz");
      doc.set("tagsList", tagsList);

      final Map<String, String> tagsMap = new HashMap<>();
      tagsMap.put("foo", "foo");
      tagsMap.put("bar", "bar");
      tagsMap.put("baz", "baz");
      doc.set("tagsMap", tagsMap);

      doc.save();
    }

    ResultSet result = database.command("sql", "update UpsertableVertex set foo = 'bar' upsert where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from UpsertableVertex");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      if ("name1".equals(name)) {
        assertThat(item.<String>getProperty("foo")).isEqualTo("bar");
      } else {
        assertThat(item.<String>getProperty("foo")).isNull();
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testLocalDateTimeUpsertWithIndex() throws ClassNotFoundException {
    if (System.getProperty("os.name").startsWith("Windows"))
      // NOTE: ON WINDOWS MICROSECONDS ARE NOT HANDLED CORRECTLY
      return;

    database.transaction(() -> {
      if (database.getSchema().existsType("Product"))
        database.getSchema().dropType("Product");

      DocumentType dtProduct = database.getSchema().createDocumentType("Product");
      dtProduct.createProperty("start", Type.DATETIME_MICROS);
      dtProduct.createProperty("stop", Type.DATETIME_MICROS);
      dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "start", "stop");
    });

    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(LocalDateTime.class);

    final LocalDateTime start = LocalDateTime.now();

    for (int i = 0; i < 10; i++) {
      database.transaction(() -> {
        final LocalDateTime stop = LocalDateTime.now();
        database.command("sql", "UPDATE Product SET start = ?, stop = ? UPSERT WHERE start = ? and stop = ?", start, stop);

        Result result;
        ResultSet resultSet = database.query("sql", "SELECT from Product");
        while (resultSet.hasNext()) {
          result = resultSet.next();
          assertThat(result.<LocalDateTime>getProperty("start")).isNotNull();
          assertThat(result.<LocalDateTime>getProperty("stop")).isNotNull();
        }
      });
    }
  }

  @Test
  public void testLocalDateTimeUpsertWithIndexMicros() throws ClassNotFoundException {
    database.transaction(() -> {
      if (database.getSchema().existsType("Product"))
        database.getSchema().dropType("Product");

      DocumentType dtProduct = database.getSchema().createDocumentType("Product");
      dtProduct.createProperty("start", Type.DATETIME_MICROS);
      dtProduct.createProperty("stop", Type.DATETIME_MICROS);
      dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "start", "stop");
    });

    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(LocalDateTime.class);

    DateTimeFormatter FILENAME_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

    database.transaction(() -> {
      LocalDateTime start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
      LocalDateTime stop = LocalDateTime.parse("20221129T002322", FILENAME_TIME_FORMAT);
      database.command("sql", "INSERT INTO Product SET start = ?, stop = ?", start, stop);
    });

    database.transaction(() -> {
      LocalDateTime start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
      LocalDateTime stop = LocalDateTime.parse("20220320T002321", FILENAME_TIME_FORMAT);
      database.command("sql", "INSERT INTO Product SET start = ?, stop = ?", start, stop);
    });

    database.transaction(() -> {
      Result result;

      /** ENTRIES:
       2022-03-18T21:55:23 - 2022-11-29T00:23:22
       2022-03-18T21:55:23 - 2022-03-20T00:23:21
       *     */
      LocalDateTime start = LocalDateTime.parse("2022-03-19T00:26:24.404379",
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
      LocalDateTime stop = LocalDateTime.parse("2022-03-19T00:28:26.525650",
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
      try (ResultSet resultSet = database.query("sql",
          "SELECT start, stop FROM Product WHERE start <= ? AND stop >= ? ORDER BY start DESC, stop DESC LIMIT 1", start, stop)) {

        assertThat(resultSet.hasNext()).isTrue();

        while (resultSet.hasNext()) {
          result = resultSet.next();
          assertThat(result.<LocalDateTime>getProperty("start")).isNotNull();
          assertThat(result.<LocalDateTime>getProperty("stop")).isNotNull();
        }
      }
    });
  }
}
