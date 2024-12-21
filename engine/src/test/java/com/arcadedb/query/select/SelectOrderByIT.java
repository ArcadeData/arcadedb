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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectOrderByIT extends TestHelper {

  public SelectOrderByIT() {
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
        database.newVertex("Vertex").set("id", i, "notIndexedId", i, "float", i + 3.14F, "name", "John").save();
      for (int i = 100; i < 110; i++)
        database.newVertex("Vertex").set("id", i, "notIndexedId", i, "name", "Jay").save();
    });
  }

  @Test
  public void okOrderByNoIndex() {
    // ASCENDING
    {
      final SelectCompiled select = database.select().fromType("Vertex").orderBy("notIndexedId", true).compile();
      int lastId = -1;
      final SelectIterator<Vertex> result = select.vertices();

      while (result.hasNext()) {
        final Integer id = result.next().getInteger("notIndexedId");
        assertThat(id > lastId).isTrue();
        lastId = id;
      }

      assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(0);
    }

    // DESCENDING
    {
      final SelectCompiled select = database.select().fromType("Vertex").orderBy("notIndexedId", false).compile();
      int lastId = Integer.MAX_VALUE;
      final SelectIterator<Vertex> result = select.vertices();

      while (result.hasNext()) {
        final Integer id = result.next().getInteger("notIndexedId");
        assertThat(id < lastId).isTrue();
        lastId = id;
      }

      assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(0);

    }
  }

  @Test
  public void okOrderBy1Index() {
    // ASCENDING
    {
      final SelectCompiled select = database.select().fromType("Vertex").where().property("id").gt().value(-1).orderBy("id", true).compile();
      int lastId = -1;
      final SelectIterator<Vertex> result = select.vertices();

      while (result.hasNext()) {
        final Integer id = result.next().getInteger("id");
        assertThat(id > lastId).isTrue();
        lastId = id;
      }

      assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    }

    // DESCENDING
    {
      final SelectCompiled select = database.select().fromType("Vertex").where().property("id").gt().value(-1).orderBy("id", false).compile();
      int lastId = Integer.MAX_VALUE;
      final SelectIterator<Vertex> result = select.vertices();

      while (result.hasNext()) {
        final Integer id = result.next().getInteger("id");
        assertThat(id < lastId).isTrue();
        lastId = id;
      }

      assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);

    }
  }
}
