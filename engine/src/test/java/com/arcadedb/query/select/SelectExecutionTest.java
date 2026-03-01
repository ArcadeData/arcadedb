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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectExecutionTest extends TestHelper {

  public SelectExecutionTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Document");
    database.getSchema().createVertexType("Vertex")//
        .createProperty("id", Type.INTEGER)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    database.getSchema().createEdgeType("Edge");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Document").set("id", i, "float", 3.14F, "name", "John").save();
        database.newVertex("Vertex").set("id", i, "float", 3.14F, "name", "John").save();
      }

      for (int i = 1; i < 100; i++) {
        final Vertex root = database.select()
            .fromType("Vertex")
            .where().property("id").eq().value(0).vertices()
            .nextOrNull();
        assertThat(root).isNotNull();
        assertThat(root.getInteger("id")).isEqualTo(0);
        root.newEdge("Edge", database.select()
                .fromType("Vertex")
                .where().property("id").eq().value(i)
                .vertices().nextOrNull())
            .save();
      }
    });
  }

  @Test
  void okFromBuckets() {
    {
      final SelectCompiled select = database.select().fromBuckets(
              database.getSchema().getType("Vertex").getBuckets(true).stream().map(Bucket::getName).collect(Collectors.toList())
                  .toArray(new String[database.getSchema().getType("Vertex").getBuckets(true).size()]))//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John").compile();

      for (int i = 0; i < 100; i++)
        assertThat(select.parameter("value", i).vertices().nextOrNull().getInteger("id")).isEqualTo(i);
    }

    {
      final SelectCompiled select = database.select().fromBuckets(
              database.getSchema().getType("Vertex").getBuckets(true).stream().map(Bucket::getFileId).collect(Collectors.toList())
                  .toArray(new Integer[database.getSchema().getType("Vertex").getBuckets(true).size()]))//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John").compile();

      for (int i = 0; i < 100; i++)
        assertThat(select.parameter("value", i).vertices().nextOrNull().getInteger("id")).isEqualTo(i);
    }
  }

  @Test
  void okAnd() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John").compile();

      for (int i = 0; i < 100; i++)
        assertThat(select.parameter("value", i).vertices().nextOrNull().getInteger("id")).isEqualTo(i);
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John2").compile();

      assertThat(select.parameter("value", 3).vertices().hasNext()).isFalse();
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().value(-1)//
          .and().property("name").eq().value("John").compile();

      assertThat(select.vertices().hasNext()).isFalse();
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John")//
          .and().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John").compile();

      for (int i = 0; i < 100; i++)
        assertThat(select.parameter("value", i).vertices().nextOrNull().getInteger("id")).isEqualTo(i);
    }
  }

  @Test
  void okOr() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("John").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        assertThat(v.getInteger("id").equals(3) || v.getString("name").equals("John")).isTrue();
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("John2").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        assertThat(v.getInteger("id").equals(3) || v.getString("name").equals("John2")).isTrue();
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().value(-1)//
          .or().property("name").eq().value("John").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        assertThat(v.getInteger("id").equals(-1) || v.getString("name").equals("John")).isTrue();
      }
    }
  }

  @Test
  void okAndOr() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John2")//
          .or().property("name").eq().value("John").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        assertThat(v.getInteger("id").equals(3) && v.getString("name").equals("John2") ||//
            v.getString("name").equals("John")).isTrue();
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("John2")//
          .and().property("name").eq().value("John").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        assertThat(v.getInteger("id").equals(3) ||//
            v.getString("name").equals("John2") && v.getString("name").equals("John")).isTrue();
      }
    }
  }

  @Test
  void okLimit() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("John").limit(10).compile();

    final SelectIterator<Vertex> iter = select.vertices();
    int browsed = 0;
    while (iter.hasNext()) {
      assertThat(iter.next().getInteger("id") < 10).isTrue();
      ++browsed;
    }
    assertThat(browsed).isEqualTo(10);
  }

  @Test
  void okSkip() {
    SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("John").skip(10).compile();

    SelectIterator<Vertex> iter = select.vertices();
    int browsed = 0;
    while (iter.hasNext()) {
      assertThat(iter.next().getInteger("id") < 10).isTrue();
      ++browsed;
    }
    assertThat(browsed).isEqualTo(0);

    select = database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("John").skip(0).compile();

    iter = select.vertices();
    browsed = 0;
    while (iter.hasNext()) {
      assertThat(iter.next().getInteger("id") < 10).isTrue();
      ++browsed;
    }
    assertThat(browsed).isEqualTo(10);

    select = database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("John").skip(2).compile();

    iter = select.vertices();
    browsed = 0;
    while (iter.hasNext()) {
      assertThat(iter.next().getInteger("id") < 10).isTrue();
      ++browsed;
    }
    assertThat(browsed).isEqualTo(8);
  }

  @Test
  void okUpdate() {
    database.transaction(() -> {
      database.select().fromType("Vertex")//
          .where().property("id").lt().value(10)//
          .and().property("name").eq().value("John")//
          .limit(10).vertices()//
          .forEachRemaining(a -> a.modify().set("modified", true).save());
    });

    database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("John").limit(10).vertices()
        .forEachRemaining(r -> assertThat(r.getInteger("id") < 10 && r.getBoolean("modified")).isTrue());
  }

  @Test
  void errorTimeout() {
    {
      expectingException(() -> {
        final SelectIterator<Vertex> iter = database.select().fromType("Vertex")//
            .where().property("name").eq().value("John").timeout(1, TimeUnit.MILLISECONDS, true).vertices();

        while (iter.hasNext()) {
          try {
            iter.next();
            Thread.sleep(2);
          } catch (InterruptedException e) {
            // IGNORE IT
          }
        }
      }, TimeoutException.class, "Timeout on iteration");
    }

    {
      final SelectIterator<Vertex> iter = database.select().fromType("Vertex")//
          .where().property("id").lt().value(10)//
          .and().property("name").eq().value("John").timeout(1, TimeUnit.MILLISECONDS, false).vertices();
    }
  }

  @Test
  void okNeq() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("id").neq().parameter("value").compile();

    for (int i = 0; i < 100; i++) {
      final int finalI = i;
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      assertThat(list.size()).isEqualTo(99);
      list.forEach(r -> Assertions.assertThat(finalI).isNotSameAs(r.getInteger("id")));
    }
  }

  /**
   * Known limitation: the parallel select cannot use the current transaction because executed in
   * different threads.
   */
  @Test
  void okParallel() {
    database.getSchema().createVertexType("Parallel");
    database.transaction(() -> {
      for (int i = 0; i < 1_000_000; i++) {
        database.newVertex("Parallel")
            .set("id", i, "float", 3.14F, "name", "Player")
            .save();
      }
    });

    final SelectCompiled select = database.select().fromType("Parallel")//
        .where().property("name").like().value("P%")
        .and()//
        .property("id").lt().value(1_000_000)//
        .compile()
        .parallel();

    Spliterator<Vertex> vertexSpliterator = Spliterators.spliteratorUnknownSize(select.vertices(), Spliterator.NONNULL);
    long size = StreamSupport.stream(vertexSpliterator, false)
        .peek(r -> assertThat(r.getString("name")).startsWith("P"))
        .count();
    assertThat(size).isEqualTo(1_000_000);
  }

  @Test
  void okLike() {
    final SelectCompiled select = database.select()
        .fromType("Vertex")//
        .where()
        .property("name").like().value("J%")
        .compile();

    for (int i = 0; i < 100; i++) {
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      assertThat(list.size()).isEqualTo(100);
      list.forEach(r -> assertThat(r.getString("name").startsWith("J")).isTrue());
    }
  }

  @Test
  void okILike() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("name").ilike().value("j%").compile();

    for (int i = 0; i < 100; i++) {
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      assertThat(list.size()).isEqualTo(100);
      list.forEach(r -> assertThat(r.getString("name").startsWith("J")).isTrue());
    }
  }

  @Test
  void okQueryMultiProperties() {
    database.command("sql", "create vertex type TestClass");

    String query = "SELECT FROM TestClass WHERE (";
    for (int i = 0; i < 20; i++) {
      database.command("sql", "create property TestClass.Property" + (i + 1) + " LINK");
      if (i > 0)
        query += " and ";
      query += "((Property" + (i + 1) + " is null) or (Property" + (i + 1) + " = #107:150))";
    }
    query += ")";

    database.query("sql", query);
  }

  @Test
  void errorMissingParameter() {
    expectingException(() -> {
      database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .vertices().nextOrNull();
    }, IllegalArgumentException.class, "Missing parameter 'value'");
  }

  @Test
  void okReuse() {
    final SelectCompiled select = database.select().fromType("Vertex").where().property("id").eq().parameter("value").compile();
    for (int i = 0; i < 100; i++)
      assertThat(select.parameter("value", i).vertices().nextOrNull().getInteger("id")).isEqualTo(i);
  }

  @Test
  void okJSON() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("John2")//
          .or().property("name").eq().value("John").compile();

      final JSONObject json = select.json();

      final JSONObject json2 = database.select().json(json).compile().json();

      assertThat(json2).isEqualTo(json);
    }
  }

  @Test
  void okCount() {
    assertThat(database.select().fromType("Vertex")//
        .where().property("name").eq().value("John").count()).isEqualTo(100);

    assertThat(database.select().fromType("Vertex")//
        .where().property("name").eq().value("NonExistent").count()).isEqualTo(0);

    assertThat(database.select().fromType("Vertex").count()).isEqualTo(100);
  }

  @Test
  void okCountWithLimit() {
    assertThat(database.select().fromType("Vertex")//
        .where().property("name").eq().value("John").limit(10).count()).isEqualTo(10);
  }

  @Test
  void okCountCompiled() {
    final SelectCompiled compiled = database.select().fromType("Vertex")//
        .where().property("id").lt().parameter("max").compile();

    assertThat(compiled.parameter("max", 10).count()).isEqualTo(10);
    assertThat(compiled.parameter("max", 50).count()).isEqualTo(50);
  }

  @Test
  void okExists() {
    assertThat(database.select().fromType("Vertex")//
        .where().property("name").eq().value("John").exists()).isTrue();

    assertThat(database.select().fromType("Vertex")//
        .where().property("name").eq().value("NonExistent").exists()).isFalse();

    assertThat(database.select().fromType("Vertex").exists()).isTrue();
  }

  @Test
  void okStream() {
    final List<String> names = database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .stream()//
        .map(d -> d.getString("name"))//
        .collect(Collectors.toList());

    assertThat(names).hasSize(10);
    names.forEach(n -> assertThat(n).isEqualTo("John"));
  }

  @Test
  void okStreamFindFirst() {
    assertThat(database.select().fromType("Vertex")//
        .where().property("id").eq().value(5)//
        .vertices().stream().findFirst()).isPresent();

    assertThat(database.select().fromType("Vertex")//
        .where().property("id").eq().value(999)//
        .vertices().stream().findFirst()).isEmpty();
  }

  @Test
  void okStreamCount() {
    final long count = database.select().fromType("Vertex")//
        .where().property("name").eq().value("John")//
        .vertices().stream().count();
    assertThat(count).isEqualTo(100);
  }

  @Test
  void okExistsCompiled() {
    final SelectCompiled compiled = database.select().fromType("Vertex")//
        .where().property("id").eq().parameter("value").compile();

    assertThat(compiled.parameter("value", 0).exists()).isTrue();
    assertThat(compiled.parameter("value", 999).exists()).isFalse();
  }

  private void expectingException(final Runnable callback, final Class<? extends Throwable> expectedException,
      final String mustContains) {
    boolean failed = true;
    try {
      callback.run();
      failed = false;
    } catch (Throwable e) {
      if (!expectedException.equals(e.getClass()))
        e.printStackTrace();

      assertThat(e.getClass()).isEqualTo(expectedException);
      assertThat(e.getMessage().contains(mustContains)).as(
          "Expected '" + mustContains + "' in the error message. Error message is: " + e.getMessage()).isTrue();
    }

    if (!failed)
      fail("Expected exception " + expectedException);
  }
}
