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
import com.arcadedb.engine.Component;
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

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectExecutionIT extends TestHelper {

  public SelectExecutionIT() {
    autoStartTx = true;
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
        database.newDocument("Document").set("id", i, "float", 3.14F, "name", "Elon").save();
        database.newVertex("Vertex").set("id", i, "float", 3.14F, "name", "Elon").save();
      }

      for (int i = 1; i < 100; i++) {
        final Vertex root = database.select().fromType("Vertex").where().property("id").eq().value(0).vertices().nextOrNull();
        Assertions.assertNotNull(root);
        Assertions.assertEquals(0, root.getInteger("id"));

        root.newEdge("Edge", database.select().fromType("Vertex").where().property("id").eq().value(i).vertices().nextOrNull(),
            true).save();
      }
    });
  }

  @Test
  public void okFromBuckets() {
    {
      final SelectCompiled select = database.select().fromBuckets(
              database.getSchema().getType("Vertex").getBuckets(true).stream().map(Component::getName).collect(Collectors.toList())
                  .toArray(new String[database.getSchema().getType("Vertex").getBuckets(true).size()]))//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon").compile();

      for (int i = 0; i < 100; i++)
        Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));
    }

    {
      final SelectCompiled select = database.select().fromBuckets(
              database.getSchema().getType("Vertex").getBuckets(true).stream().map(Bucket::getFileId).collect(Collectors.toList())
                  .toArray(new Integer[database.getSchema().getType("Vertex").getBuckets(true).size()]))//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon").compile();

      for (int i = 0; i < 100; i++)
        Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));
    }
  }

  @Test
  public void okAnd() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon").compile();

      for (int i = 0; i < 100; i++)
        Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon2").compile();

      Assertions.assertFalse(select.parameter("value", 3).vertices().hasNext());
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().value(-1)//
          .and().property("name").eq().value("Elon").compile();

      Assertions.assertFalse(select.vertices().hasNext());
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon")//
          .and().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon").compile();

      for (int i = 0; i < 100; i++)
        Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));
    }
  }

  @Test
  public void okOr() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("Elon").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        Assertions.assertTrue(v.getInteger("id").equals(3) || v.getString("name").equals("Elon"));
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("Elon2").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        Assertions.assertTrue(v.getInteger("id").equals(3) || v.getString("name").equals("Elon2"));
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().value(-1)//
          .or().property("name").eq().value("Elon").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        Assertions.assertTrue(v.getInteger("id").equals(-1) || v.getString("name").equals("Elon"));
      }
    }
  }

  @Test
  public void okAndOr() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon2")//
          .or().property("name").eq().value("Elon").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        Assertions.assertTrue(v.getInteger("id").equals(3) && v.getString("name").equals("Elon2") ||//
            v.getString("name").equals("Elon"));
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("Elon2")//
          .and().property("name").eq().value("Elon").compile();

      for (SelectIterator<Vertex> result = select.parameter("value", 3).vertices(); result.hasNext(); ) {
        final Vertex v = result.next();
        Assertions.assertTrue(v.getInteger("id").equals(3) ||//
            v.getString("name").equals("Elon2") && v.getString("name").equals("Elon"));
      }
    }
  }

  @Test
  public void okLimit() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("Elon").limit(10).compile();

    final SelectIterator<Vertex> iter = select.vertices();
    int browsed = 0;
    while (iter.hasNext()) {
      Assertions.assertTrue(iter.next().getInteger("id") < 10);
      ++browsed;
    }
    Assertions.assertEquals(10, browsed);
  }

  @Test
  public void okUpdate() {
    database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("Elon")//
        .limit(10).vertices()//
        .forEachRemaining(a -> a.modify().set("modified", true).save());

    database.select().fromType("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("Elon").limit(10).vertices()
        .forEachRemaining(r -> Assertions.assertTrue(r.getInteger("id") < 10 && r.getBoolean("modified")));
  }

  @Test
  public void errorTimeout() {
    {
      expectingException(() -> {
        final SelectIterator<Vertex> iter = database.select().fromType("Vertex")//
            .where().property("name").eq().value("Elon").timeout(1, TimeUnit.MILLISECONDS, true).vertices();

        while (iter.hasNext()) {
          Assertions.assertTrue(iter.next().getInteger("id") < 10);
          try {
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
          .and().property("name").eq().value("Elon").timeout(1, TimeUnit.MILLISECONDS, false).vertices();
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
      Assertions.assertEquals(99, list.size());
      list.forEach(r -> Assertions.assertTrue(r.getInteger("id") != finalI));
    }
  }

  @Test
  public void okLike() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("name").like().value("E%").compile();

    for (int i = 0; i < 100; i++) {
      final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
      final List<Vertex> list = result.toList();
      Assertions.assertEquals(100, list.size());
      list.forEach(r -> Assertions.assertTrue(r.getString("name").startsWith("E")));
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
    final SelectCompiled select = database.select().fromType("Vertex").where().property("id").eq().parameter("value")
        .compile();
    for (int i = 0; i < 100; i++)
      Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));
  }

  @Test
  public void okJSON() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon2")//
          .or().property("name").eq().value("Elon").compile();

      final JSONObject json = select.json();

      final JSONObject json2 = database.select().json(json).compile().json();

      Assertions.assertEquals(json, json2);
    }
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

      Assertions.assertEquals(expectedException, e.getClass());
      Assertions.assertTrue(e.getMessage().contains(mustContains),
          "Expected '" + mustContains + "' in the error message. Error message is: " + e.getMessage());
    }

    if (!failed)
      Assertions.fail("Expected exception " + expectedException);
  }
}
