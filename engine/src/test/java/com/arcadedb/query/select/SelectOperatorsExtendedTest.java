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
 * Tests for IN, BETWEEN, IS NULL, IS NOT NULL operators in the Select API.
 */
public class SelectOperatorsExtendedTest extends TestHelper {

  public SelectOperatorsExtendedTest() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    final VertexType v = database.getSchema().createVertexType("Vertex");
    v.createProperty("id", Type.INTEGER)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    v.createProperty("name", Type.STRING)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    v.createProperty("status", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final String status = i < 30 ? "A" : (i < 70 ? "B" : "C");
        database.newVertex("Vertex").set("id", i, "name", "John", "status", status).save();
      }
      // Records with null name
      for (int i = 100; i < 110; i++)
        database.newVertex("Vertex").set("id", i, "status", "D").save();
    });
  }

  @Test
  void okInWithoutIndex() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("status").in().value(List.of("A", "B"))//
        .vertices().toList();

    assertThat(result).hasSize(70);
    result.forEach(r -> assertThat(r.getString("status")).isIn("A", "B"));
  }

  @Test
  void okInWithIndex() {
    final SelectIterator<Vertex> result = database.select().fromType("Vertex")//
        .where().property("id").in().value(List.of(0, 5, 10, 99))//
        .vertices();

    final List<Vertex> list = result.toList();
    assertThat(list).hasSize(4);
    list.forEach(r -> assertThat(r.getInteger("id")).isIn(0, 5, 10, 99));
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
  }

  @Test
  void okInWithIndexedName() {
    final SelectIterator<Vertex> result = database.select().fromType("Vertex")//
        .where().property("name").in().value(List.of("John", "Jane"))//
        .vertices();

    final List<Vertex> list = result.toList();
    assertThat(list).hasSize(100);
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
  }

  @Test
  void okInEmptyCollection() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("status").in().value(List.of())//
        .vertices().toList();

    assertThat(result).isEmpty();
  }

  @Test
  void okBetweenWithoutIndex() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("status").between().values("A", "B")//
        .vertices().toList();

    assertThat(result).hasSize(70);
    result.forEach(r -> {
      final String s = r.getString("status");
      assertThat(s.compareTo("A") >= 0 && s.compareTo("B") <= 0).isTrue();
    });
  }

  @Test
  void okBetweenWithIndex() {
    final SelectIterator<Vertex> result = database.select().fromType("Vertex")//
        .where().property("id").between().values(10, 20)//
        .vertices();

    final List<Vertex> list = result.toList();
    assertThat(list).hasSize(11);
    list.forEach(r -> {
      assertThat(r.getInteger("id")).isBetween(10, 20);
    });
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
  }

  @Test
  void okIsNull() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("name").isNull()//
        .vertices().toList();

    assertThat(result).hasSize(10);
    result.forEach(r -> assertThat(r.has("name")).isFalse());
  }

  @Test
  void okIsNotNull() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("name").isNotNull()//
        .vertices().toList();

    assertThat(result).hasSize(100);
    result.forEach(r -> assertThat(r.getString("name")).isNotNull());
  }

  @Test
  void okIsNotNullAndOtherCondition() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("name").isNotNull()//
        .and().property("id").gt().value(50)//
        .vertices().toList();

    assertThat(result).hasSize(49);
    result.forEach(r -> {
      assertThat(r.getString("name")).isNotNull();
      assertThat(r.getInteger("id")).isGreaterThan(50);
    });
  }

  @Test
  void okIsNullOrCondition() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("name").isNull()//
        .or().property("id").eq().value(0)//
        .vertices().toList();

    assertThat(result).hasSize(11);
  }

  @Test
  void okBetweenAndIn() {
    final List<Vertex> result = database.select().fromType("Vertex")//
        .where().property("id").between().values(0, 50)//
        .and().property("status").in().value(List.of("A", "C"))//
        .vertices().toList();

    assertThat(result).hasSize(30);
    result.forEach(r -> {
      assertThat(r.getInteger("id")).isBetween(0, 50);
      assertThat(r.getString("status")).isIn("A", "C");
    });
  }

  @Test
  void okInWithParameter() {
    final SelectCompiled select = database.select().fromType("Vertex")//
        .where().property("status").in().parameter("values").compile();

    final List<Vertex> result = select.parameter("values", List.of("A", "C")).vertices().toList();
    assertThat(result).hasSize(60);
    result.forEach(r -> assertThat(r.getString("status")).isIn("A", "C"));
  }
}
