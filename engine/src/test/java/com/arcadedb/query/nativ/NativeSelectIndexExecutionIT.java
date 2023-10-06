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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelectIndexExecutionIT extends TestHelper {

  public NativeSelectIndexExecutionIT() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    final VertexType v = database.getSchema().createVertexType("Vertex");
    v.createProperty("id", Type.INTEGER)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    v.createProperty("name", Type.STRING)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newVertex("Vertex").set("id", i, "float", 3.14F, "name", "Elon").save();
      for (int i = 100; i < 110; i++)
        database.newVertex("Vertex").set("id", i, "name", "Jay").save();
    });
  }

  @Test
  public void okOneOfTwoAvailableIndexes() {
    // EXPECTED TO USE BOTH INDEXES BECAUSE OF THE AND LOGIC OPERATOR
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();

        final List<Vertex> list = result.toList();
        Assertions.assertEquals(i < 100 ? 1 : 0, list.size());

        list.forEach(r -> Assertions.assertTrue(r.getInteger("id") == finalI && r.getString("name").equals("Elon")));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(1L, result.getMetrics().get("evaluatedRecords"), "With id " + i);
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }
  }

  @Test
  public void okBothAvailableIndexes() {
    // EXPECTED TO USE BOTH INDEXES BECAUSE OF THE OR LOGIC OPERATOR AND EACH PROPERTY IS INDEXED
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("Elon");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();

        result.forEachRemaining(r -> Assertions.assertTrue(r.getInteger("id") == finalI || r.getString("name").equals("Elon")));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(i < 100 ? 100L : 101L, result.getMetrics().get("evaluatedRecords"), "" + finalI);
        Assertions.assertEquals(2, result.getMetrics().get("indexesUsed"));
      }
    }
  }

  @Test
  public void okOneIndexUsed() {
    // EXPECTED TO USE ONLY ONE INDEX
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("unknown").eq().value(null);

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();

        result.forEachRemaining(r -> Assertions.assertEquals((int) r.getInteger("id"), finalI));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(1L, result.getMetrics().get("evaluatedRecords"));
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }

    // EXPECTED TO USE ONLY ONE INDEX
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("unknown").eq().value(null)//
          .and().property("id").eq().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();

        result.forEachRemaining(r -> Assertions.assertEquals((int) r.getInteger("id"), finalI));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(1L, result.getMetrics().get("evaluatedRecords"));
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }
  }

  @Test
  public void okNoIndexUsed() {
    // EXPECTED NO INDEXES IS USED BECAUSE NO INDEXES WERE DEFINED ON ANY OF THE PROPERTIES
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("unknown").eq().value(null)//
          .and().property("unknown").eq().value(null);

      for (int i = 0; i < 110; i++) {
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        result.toList();

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(110L, result.getMetrics().get("evaluatedRecords"));
        Assertions.assertEquals(0, result.getMetrics().get("indexesUsed"));
      }
    }

    // EXPECTED NO INDEXES IS USED BECAUSE THE OR OPERATOR ONLY ONE ONE PROPERTY
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("unknown").eq().value(null);

      for (int i = 0; i < 110; i++) {
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        result.toList();

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(110L, result.getMetrics().get("evaluatedRecords"));
        Assertions.assertEquals(0, result.getMetrics().get("indexesUsed"));
      }
    }

    // EXPECTED NO INDEXES IS USED BECAUSE THE OR OPERATOR ONLY ONE ONE PROPERTY
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("unknown").eq().value(null).and().property("id").eq().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;

        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();

        final List<Vertex> list = result.toList();
        Assertions.assertEquals(1, list.size());

        list.forEach(r -> Assertions.assertEquals((int) r.getInteger("id"), finalI));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        Assertions.assertEquals(1L, result.getMetrics().get("evaluatedRecords"));
        Assertions.assertEquals(2, result.getMetrics().get("indexesUsed"));
      }
    }

  }

  @Test
  public void okRanges() {
    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").gt().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        Assertions.assertEquals(109 - i, list.size());
        list.forEach(r -> Assertions.assertTrue(r.getInteger("id") > finalI));
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }

    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").ge().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        Assertions.assertEquals(110 - i, list.size());
        list.forEach(r -> Assertions.assertTrue(r.getInteger("id") >= finalI));
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }

    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").lt().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        Assertions.assertEquals(i, list.size());
        list.forEach(r -> Assertions.assertTrue(r.getInteger("id") < finalI));
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }

    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").le().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        Assertions.assertEquals(i + 1, list.size());
        list.forEach(r -> Assertions.assertTrue(r.getInteger("id") <= finalI));
        Assertions.assertEquals(1, result.getMetrics().get("indexesUsed"));
      }
    }

    {
      final NativeSelect select = database.select().fromType("Vertex")//
          .where().property("id").neq().parameter("value");

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final QueryIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        Assertions.assertEquals(109, list.size());
        list.forEach(r -> Assertions.assertTrue(r.getInteger("id") != finalI));
        Assertions.assertEquals(0, result.getMetrics().get("indexesUsed"));
      }
    }
  }
}
