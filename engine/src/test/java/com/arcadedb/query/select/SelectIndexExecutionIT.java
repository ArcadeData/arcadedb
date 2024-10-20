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
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectIndexExecutionIT extends TestHelper {

  public SelectIndexExecutionIT() {
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
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("name").eq().value("Elon").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();

        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(i < 100 ? 1 : 0);

        list.forEach(r -> assertThat(r.getInteger("id") == finalI && r.getString("name").equals("Elon")).isTrue());

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).as("With id " + i).isEqualTo(1L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }
  }

  @Test
  public void okBothAvailableIndexes() {
    // EXPECTED TO USE BOTH INDEXES BECAUSE OF THE OR LOGIC OPERATOR AND EACH PROPERTY IS INDEXED
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("name").eq().value("Elon").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();

        result.forEachRemaining(r -> assertThat(r.getInteger("id") == finalI || r.getString("name").equals("Elon")).isTrue());

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).as("" + finalI).isEqualTo(i < 100 ? 100L : 101L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(2);
      }
    }
  }

  @Test
  public void okOneIndexUsed() {
    // EXPECTED TO USE ONLY ONE INDEX
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .and().property("unknown").eq().value(null).compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();

        result.forEachRemaining(r -> assertThat((int) r.getInteger("id")).isEqualTo(finalI));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }

    // EXPECTED TO USE ONLY ONE INDEX
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("unknown").eq().value(null)//
          .and().property("id").eq().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();

        result.forEachRemaining(r -> assertThat((int) r.getInteger("id")).isEqualTo(finalI));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }
  }

  @Test
  public void okNoIndexUsed() {
    // EXPECTED NO INDEXES IS USED BECAUSE NO INDEXES WERE DEFINED ON ANY OF THE PROPERTIES
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("unknown").eq().value(null)//
          .and().property("unknown").eq().value(null).compile();

      for (int i = 0; i < 110; i++) {
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        result.toList();

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(110L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(0);
      }
    }

    // EXPECTED NO INDEXES IS USED BECAUSE THE OR OPERATOR ONLY ONE ONE PROPERTY
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("unknown").eq().value(null).compile();

      for (int i = 0; i < 110; i++) {
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        result.toList();

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(110L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(0);
      }
    }

    // EXPECTED NO INDEXES IS USED BECAUSE THE OR OPERATOR ONLY ONE ONE PROPERTY
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").eq().parameter("value")//
          .or().property("unknown").eq().value(null).and().property("id").eq().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;

        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();

        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(1);

        list.forEach(r -> assertThat((int) r.getInteger("id")).isEqualTo(finalI));

        // CHECK 1 FOR ID = I + 100 FOR NAME = ELON (ALL OF THEM)
        assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(2);
      }
    }

  }

  @Test
  public void okRanges() {
    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").gt().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(109 - i);
        list.forEach(r -> assertThat(r.getInteger("id")).isGreaterThan(finalI));
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").ge().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(110 - i);
        list.forEach(r -> assertThat(r.getInteger("id")).isGreaterThanOrEqualTo(finalI));
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").lt().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(i);
        list.forEach(r -> assertThat(r.getInteger("id")).isLessThan(finalI));
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").le().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(i + 1);
        list.forEach(r -> assertThat(r.getInteger("id")).isLessThanOrEqualTo(finalI));
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
      }
    }

    {
      final SelectCompiled select = database.select().fromType("Vertex")//
          .where().property("id").neq().parameter("value").compile();

      for (int i = 0; i < 110; i++) {
        final int finalI = i;
        final SelectIterator<Vertex> result = select.parameter("value", i).vertices();
        final List<Vertex> list = result.toList();
        assertThat(list.size()).isEqualTo(109);
        list.forEach(r -> assertThat(r.getInteger("id")).isNotEqualTo(finalI));
        assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(0);
      }
    }
  }
}
