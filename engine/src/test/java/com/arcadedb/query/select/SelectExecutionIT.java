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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectExecutionIT extends TestHelper {

  public SelectExecutionIT() {
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
        final Vertex root = database.select().fromType("Vertex").where().property("id").eq().value(0).vertices().nextOrNull();
        assertNotNull(root);
        assertEquals(0, root.getInteger("id"));

        root.newEdge("Edge", database.select().fromType("Vertex").where().property("id").eq().value(i).vertices().nextOrNull(),
            true).save();
      }
    });
  }

  @Test
  public void okFromBuckets() {
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
  public void okAnd() {
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
  public void okOr() {
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
  public void okAndOr() {
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
  public void okLimit() {
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
  public void okSkip() {
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
  public void okUpdate() {
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
        .forEachRemaining(r -> assertTrue(r.getInteger("id") < 10 && r.getBoolean("modified")));
  }

  @Test
  public void errorTimeout() {
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
  public void okNeq() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("id").neq().parameter("value").compile();

    for (int i = 0; i < 100; i++) {
      final int finalI = i;
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      assertThat(list.size()).isEqualTo(99);
      list.forEach(r -> assertTrue(r.getInteger("id") != finalI));
    }
  }

  /**
   * Known limitation: the parallel select cannot use the current transaction because executed in
   * different threads.
   */
  @Test
  public void okParallel() {
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
  public void okLike() {
    final SelectCompiled select = database.select()
        .fromType("Vertex")//
        .where()
        .property("name").like().value("J%")
        .compile();

    for (int i = 0; i < 100; i++) {
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      assertThat(list.size()).isEqualTo(100);
      list.forEach(r -> assertTrue(r.getString("name").startsWith("J")));
    }
  }

  @Test
  public void okILike() {
    final SelectCompiled select = database.select()
        .fromType("Vertex")//
        .where()
        .property("name").ilike().value("j%")
        .compile();

    for (int i = 0; i < 100; i++) {
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      assertThat(list.size()).isEqualTo(100);
      list.forEach(r -> assertTrue(r.getString("name").startsWith("J")));
    }
  }

  @Test
  public void errorMissingParameter() {
    expectingException(() -> {
      database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .vertices().nextOrNull();
    }, IllegalArgumentException.class, "Missing parameter 'value'");
  }

  @Test
  public void okReuse() {
    final SelectCompiled select = database.select().fromType("Vertex").where().property("id").eq().parameter("value").compile();
    for (int i = 0; i < 100; i++)
      assertThat(select.parameter("value", i).vertices().nextOrNull().getInteger("id")).isEqualTo(i);
  }

  @Test
  public void okJSON() {
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

  private void expectingException(final Runnable callback, final Class<? extends Throwable> expectedException,
      final String mustContains) {
    boolean failed = true;
    try {
      callback.run();
      failed = false;
    } catch (Throwable e) {
      if (!expectedException.equals(e.getClass())) e.printStackTrace();

      assertThat(e.getClass()).isEqualTo(expectedException);
      assertThat(e.getMessage().contains(mustContains)).as(
          "Expected '" + mustContains + "' in the error message. Error message is: " + e.getMessage()).isTrue();
    }

    if (!failed) fail("Expected exception " + expectedException);
  }
}
