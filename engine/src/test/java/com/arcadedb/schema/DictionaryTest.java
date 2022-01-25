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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class DictionaryTest extends TestHelper {
  @Test
  public void updateName() {
    database.transaction(() -> {
      Assertions.assertFalse(database.getSchema().existsType("V"));

      final DocumentType type = database.getSchema().createDocumentType("V", 3);
      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);

      for (int i = 0; i < 10; ++i) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }
    });

    Assertions.assertEquals(4, database.getSchema().getDictionary().getDictionaryMap().size());

    database.transaction(() -> {
      Assertions.assertTrue(database.getSchema().existsType("V"));

      final MutableDocument v = database.newDocument("V");
      v.set("id", 10);
      v.set("name", "Jay");
      v.set("surname", "Miner");
      v.set("newProperty", "newProperty");
      v.save();
    });

    Assertions.assertEquals(5, database.getSchema().getDictionary().getDictionaryMap().size());

    database.transaction(() -> {
      Assertions.assertTrue(database.getSchema().existsType("V"));
      database.getSchema().getDictionary().updateName("name", "firstName");
    });

    Assertions.assertEquals(5, database.getSchema().getDictionary().getDictionaryMap().size());

    database.transaction(() -> {
      final ResultSet iter = database.query("sql", "select from V order by id asc");

      int i = 0;
      while (iter.hasNext()) {
        final Document d = (Document) iter.next().getRecord().get();

        Assertions.assertEquals(i, d.getInteger("id"));
        Assertions.assertEquals("Jay", d.getString("firstName"));
        Assertions.assertEquals("Miner", d.getString("surname"));

        if (i == 10)
          Assertions.assertEquals("newProperty", d.getString("newProperty"));
        else
          Assertions.assertNull(d.getString("newProperty"));

        Assertions.assertNull(d.getString("name"));

        ++i;
      }

      Assertions.assertEquals(11, i);
    });

    try {
      database.transaction(() -> {
        Assertions.assertTrue(database.getSchema().existsType("V"));
        database.getSchema().getDictionary().updateName("V", "V2");
      });
      Assertions.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void namesClash() {
    database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      int finalI = i;
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Babylonia");
        for (int k = 0; k < 10; k++) {
          v.set("origin", finalI);
          v.set("p" + ((finalI * 10) + k), ((finalI * 10) + k));
        }
        v.save();
      });
    }

    for (Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      Assertions.assertEquals(11, v.getPropertyNames().size());

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        Assertions.assertEquals((origin * 10) + k, value);
      }
    }
  }

  @Test
  public void namesClashPropertyCreatedOnSchemaBefore() {
    final VertexType babylonia = database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      int finalI = i;

      for (int k = 1; k < 10; k++)
        babylonia.createProperty("p" + ((finalI * 10) + k), Type.INTEGER);

      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Babylonia");
        for (int k = 0; k < 10; k++) {
          v.set("origin", finalI);
          v.set("p" + ((finalI * 10) + k), ((finalI * 10) + k));
        }
        v.save();
      });
    }

    for (Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      Assertions.assertEquals(11, v.getPropertyNames().size());

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        Assertions.assertEquals((origin * 10) + k, value);
      }
    }
  }

  @Test
  public void namesClashPropertyCreatedOnSchemaSameTx() {
    final VertexType babylonia = database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      int finalI = i;

      database.transaction(() -> {
        for (int k = 1; k < 10; k++)
          babylonia.createProperty("p" + ((finalI * 10) + k), Type.INTEGER);

        final MutableVertex v = database.newVertex("Babylonia");
        for (int k = 0; k < 10; k++) {
          v.set("origin", finalI);
          v.set("p" + ((finalI * 10) + k), ((finalI * 10) + k));
        }
        v.save();
      });
    }

    for (Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      Assertions.assertEquals(11, v.getPropertyNames().size());

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        Assertions.assertEquals((origin * 10) + k, value);
      }
    }
  }

  @Test
  public void namesClashPropertyCreatedOnSchemaSubTx() {
    final VertexType babylonia = database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      int finalI = i;

      database.transaction(() -> {
        for (int k = 1; k < 10; k++)
          babylonia.createProperty("p" + ((finalI * 10) + k), Type.INTEGER);

        database.transaction(() -> {
          final MutableVertex v = database.newVertex("Babylonia");
          for (int k = 0; k < 10; k++) {
            v.set("origin", finalI);
            v.set("p" + ((finalI * 10) + k), ((finalI * 10) + k));
          }
          v.save();
        });
      });
    }

    for (Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      Assertions.assertEquals(11, v.getPropertyNames().size());

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        Assertions.assertEquals((origin * 10) + k, value);
      }
    }
  }
}
