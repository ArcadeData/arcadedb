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
package com.arcadedb.query.nativ;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.Callable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelectExecutionIT extends TestHelper {

  public NativeSelectExecutionIT() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Document");
    database.getSchema().createVertexType("Vertex");
    database.getSchema().createEdgeType("Edge");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Document").set("id", i, "float", 3.14F, "name", "Elon").save();
        database.newVertex("Vertex").set("id", i, "float", 3.14F, "name", "Elon").save();
      }

      for (int i = 1; i < 100; i++) {
        final Vertex root = database.select().from("Vertex").where().property("id").eq().value(0).vertices().nextOrNull();
        Assertions.assertNotNull(root);
        Assertions.assertEquals(0, root.getInteger("id"));

        root.newEdge("Edge", database.select().from("Vertex").where().property("id").eq().value(i).vertices().nextOrNull(), true)
            .save();
      }
    });
  }

  @Test
  public void okReuse() {
    final NativeSelect select = database.select().from("Vertex").where().property("id").eq().parameter("value");
    for (int i = 0; i < 100; i++)
      Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));
  }

  @Test
  public void okAnd() {
    final NativeSelect select = database.select().from("Vertex")//
        .where().property("id").eq().parameter("value")//
        .and().property("name").eq().value("Elon");

    for (int i = 0; i < 100; i++)
      Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));

    final NativeSelect select2 = database.select().from("Vertex")//
        .where().property("id").eq().parameter("value")//
        .and().property("name").eq().value("Elon2");

    Assertions.assertFalse(select2.parameter("value", 3).vertices().hasNext());

    final NativeSelect select3 = database.select().from("Vertex")//
        .where().property("id").eq().value(-1)//
        .and().property("name").eq().value("Elon");

    Assertions.assertFalse(select3.vertices().hasNext());
  }

  @Test
  public void okOr() {
    final NativeSelect select = database.select().from("Vertex")//
        .where().property("id").eq().parameter("value")//
        .or().property("name").eq().value("Elon2");

    Assertions.assertEquals(3, select.parameter("value", 3).vertices().nextOrNull().getInteger("id"));

    final NativeSelect select2 = database.select().from("Vertex")//
        .where().property("id").eq().parameter("value")//
        .or().property("name").eq().value("Elon2");

    Assertions.assertEquals(3, select2.parameter("value", 3).vertices().nextOrNull().getInteger("id"));

    final NativeSelect select3 = database.select().from("Vertex")//
        .where().property("id").eq().value(-1)//
        .or().property("name").eq().value("Elon");

    Assertions.assertEquals(0, select3.parameter("value", 3).vertices().nextOrNull().getInteger("id"));
  }

  @Test
  public void okAndOr() {
    final NativeSelect select = database.select().from("Vertex")//
        .where().property("id").eq().parameter("value")//
        .and().property("name").eq().value("Elon")//
        .and().property("id").eq().parameter("value")//
        .and().property("name").eq().value("Elon");

    for (int i = 0; i < 100; i++)
      Assertions.assertEquals(i, select.parameter("value", i).vertices().nextOrNull().getInteger("id"));

    final NativeSelect select2 = database.select().from("Vertex")//
        .where().property("id").eq().parameter("value")//
        .and().property("name").eq().value("Elon2");

    Assertions.assertFalse(select2.parameter("value", 3).vertices().hasNext());

    final NativeSelect select3 = database.select().from("Vertex")//
        .where().property("id").eq().value(-1)//
        .and().property("name").eq().value("Elon");

    Assertions.assertFalse(select3.vertices().hasNext());

  }

  @Test
  public void okLimit() {
    final NativeSelect select = database.select().from("Vertex")//
        .where().property("id").lt().value(10)//
        .and().property("name").eq().value("Elon").limit(10);

    final QueryIterator<Vertex> iter = select.vertices();
    while (iter.hasNext()) {
      Assertions.assertTrue(iter.next().getInteger("id") < 10);
    }
    Assertions.assertFalse(iter.hasNext());
  }

  @Test
  public void errorMissingParameter() {
    expectingException(() -> {
      database.select().from("Vertex")//
          .where().property("id").eq().parameter("value")//
          .vertices().nextOrNull();
    }, IllegalArgumentException.class, "Missing parameter 'value'");
  }

  @Test
  public void errorMissingCondition() {
    expectingException(() -> {
      database.select().from("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().vertices().nextOrNull();
    }, IllegalArgumentException.class, "Missing condition");
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
