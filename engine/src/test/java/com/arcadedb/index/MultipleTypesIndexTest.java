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
import org.junit.jupiter.api.Assertions;

import java.util.*;

public class MultipleTypesIndexTest extends TestHelper {
  private static final int    TOT       = 100000;
  private static final String TYPE_NAME = "Profile";

  //@Test
  public void testCollection() {
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[keywords]");

      IndexCursor cursor = index.get(new Object[] { List.of("ceo", "tesla", "spacex", "boring", "neuralink", "twitter") });
      Assertions.assertTrue(cursor.hasNext());
      Assertions.assertEquals("Musk", cursor.next().asVertex().getString("lastName"));
      Assertions.assertFalse(cursor.hasNext());

      ResultSet resultset = database.query("sql", "select from " + TYPE_NAME + " where keywords CONTAINS ?", "tesla");
      Assertions.assertTrue(resultset.hasNext());
      Assertions.assertEquals("Musk", resultset.next().toElement().asVertex().getString("lastName"));
      Assertions.assertFalse(resultset.hasNext());

      resultset = database.query("sql", "select from " + TYPE_NAME + " where 'tesla' IN  keywords");
      Assertions.assertTrue(resultset.hasNext());
      Assertions.assertEquals("Musk", resultset.next().toElement().asVertex().getString("lastName"));
      Assertions.assertFalse(resultset.hasNext());

      resultset = database.query("sql", "select from " + TYPE_NAME + " where ? IN keywords", "tesla");
      Assertions.assertTrue(resultset.hasNext());
      Assertions.assertEquals("Musk", resultset.next().toElement().asVertex().getString("lastName"));
      Assertions.assertFalse(resultset.hasNext());

      cursor = index.get(new Object[] { List.of("inventor", "commodore", "amiga", "atari", "80s") });
      Assertions.assertTrue(cursor.hasNext());
      Assertions.assertEquals("Jay", cursor.next().asVertex().getString("firstName"));
      Assertions.assertFalse(cursor.hasNext());

      cursor = index.get(new Object[] { List.of("writer") });
      Assertions.assertTrue(cursor.hasNext());

      int i = 0;
      for (; cursor.hasNext(); i++) {
        cursor.next();
      }
      Assertions.assertEquals(TOT - 2, i);
    });
  }

  protected void beginTest() {
    database.transaction(() -> {
      Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

      final DocumentType type = database.getSchema().createVertexType(TYPE_NAME, 3);
      type.createProperty("id", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" });
      type.createProperty("firstName", String.class);
      type.createProperty("lastName", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "firstName", "lastName" });
      type.createProperty("keywords", List.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "keywords" });

      MutableVertex v = database.newVertex(TYPE_NAME);
      v.set("id", 0);
      v.set("firstName", "Elon");
      v.set("lastName", "Musk");
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
