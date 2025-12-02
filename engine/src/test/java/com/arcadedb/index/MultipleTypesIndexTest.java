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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MultipleTypesIndexTest extends TestHelper {
  private static final int    TOT       = 100000;
  private static final String TYPE_NAME = "Profile";

  @Test
  void collection() {
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[keywords]");

      IndexCursor cursor = index.get(new Object[] { List.of("ceo", "tesla", "spacex", "boring", "neuralink", "twitter") });
      assertThat(cursor.hasNext()).isTrue();
      assertThat(cursor.next().asVertex().getString("lastName")).isEqualTo("Red");
      assertThat(cursor.hasNext()).isFalse();

      ResultSet resultset = database.query("sql", "select from " + TYPE_NAME + " where keywords CONTAINS ?", "tesla");
      assertThat(resultset.hasNext()).isTrue();
      assertThat(resultset.next().toElement().asVertex().getString("lastName")).isEqualTo("Red");
      assertThat(resultset.hasNext()).isFalse();

      resultset = database.query("sql", "select from " + TYPE_NAME + " where 'tesla' IN  keywords");
      assertThat(resultset.hasNext()).isTrue();
      assertThat(resultset.next().toElement().asVertex().getString("lastName")).isEqualTo("Red");
      assertThat(resultset.hasNext()).isFalse();

      resultset = database.query("sql", "select from " + TYPE_NAME + " where ? IN keywords", "tesla");
      assertThat(resultset.hasNext()).isTrue();
      assertThat(resultset.next().toElement().asVertex().getString("lastName")).isEqualTo("Red");
      assertThat(resultset.hasNext()).isFalse();

      cursor = index.get(new Object[] { List.of("inventor", "commodore", "amiga", "atari", "80s") });
      assertThat(cursor.hasNext()).isTrue();
      assertThat(cursor.next().asVertex().getString("firstName")).isEqualTo("Jay");
      assertThat(cursor.hasNext()).isFalse();

      cursor = index.get(new Object[] { List.of("writer") });
      assertThat(cursor.hasNext()).isTrue();

      int i = 0;
      for (; cursor.hasNext(); i++) {
        cursor.next();
      }
      assertThat(i).isEqualTo(TOT - 2);
    });
  }

  @Test
  void nullItemInCollection() {
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[keywords]");

      MutableVertex v = database.newVertex(TYPE_NAME);
      v.set("id", TOT + 1);
      v.set("firstName", "Jake");
      v.set("lastName", "White");

      final List<Object> list = new ArrayList<>();
      list.add(null);
      v.set("keywords", list);

      v.save();

      IndexCursor cursor = index.get(new Object[] { list });
      assertThat(cursor.hasNext()).isTrue();
      assertThat(cursor.next().asVertex().getString("lastName")).isEqualTo("White");
      assertThat(cursor.hasNext()).isFalse();
    });
  }

  // Issue https://github.com/ArcadeData/arcadedb/issues/812
  @Test
  void updateCompositeKeyIndex() {
    VertexType type = database.getSchema().createVertexType("IndexedVertex");
    type.createProperty("counter", Type.INTEGER);
    type.createProperty("status", Type.STRING);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "counter");

    database.transaction(() -> {
      database.newVertex("IndexedVertex").set("id", "test1").set("status", "on").set("counter", 1).save();
      database.newVertex("IndexedVertex").set("id", "test2").set("status", "on").set("counter", 2).save();
      database.newVertex("IndexedVertex").set("id", "test3").set("status", "on").set("counter", 3).save();

      database.command("SQL", "update IndexedVertex set status = 'off' where counter = 2");
      database.command("SQL", "update IndexedVertex set status = 'off' where counter = 3");
    });
  }

  protected void beginTest() {
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

      final DocumentType type = database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(3).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .create();
      type.createProperty("firstName", String.class);
      type.createProperty("lastName", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "firstName", "lastName" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      type.createProperty("keywords", List.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "keywords" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false).create();

      MutableVertex v = database.newVertex(TYPE_NAME);
      v.set("id", 0);
      v.set("firstName", "John");
      v.set("lastName", "Red");
      v.set("keywords", List.of("ceo", "tesla", "spacex", "boring", "neuralink", "twitter"));
      v.save();

      v = database.newVertex(TYPE_NAME);
      v.set("id", 1);
      v.set("firstName", "Jay");
      v.set("lastName", "Miner");
      v.set("keywords", List.of("inventor", "commodore", "amiga", "atari", "80s"));
      v.save();

      for (int i = 2; i < TOT; i++) {
        v = database.newVertex(TYPE_NAME);
        v.set("id", i);
        v.set("firstName", "Random");
        v.set("lastName", "Guy");
        v.set("keywords", List.of("writer"));
        v.save();
      }

      database.commit();
      database.begin();
    });
  }
}
