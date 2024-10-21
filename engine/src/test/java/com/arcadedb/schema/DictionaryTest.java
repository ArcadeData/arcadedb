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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DictionaryTest extends TestHelper {
  @Test
  public void updateName() {
    database.transaction(() -> {
      Assertions.assertThat(database.getSchema().existsType("V")).isFalse();

      final DocumentType type = database.getSchema().buildDocumentType().withName("V").withTotalBuckets(3).create();
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

    assertThat(database.getSchema().getDictionary().getDictionaryMap().size()).isEqualTo(4);

    database.transaction(() -> {
      Assertions.assertThat(database.getSchema().existsType("V")).isTrue();

      final MutableDocument v = database.newDocument("V");
      v.set("id", 10);
      v.set("name", "Jay");
      v.set("surname", "Miner");
      v.set("newProperty", "newProperty");
      v.save();
    });

    assertThat(database.getSchema().getDictionary().getDictionaryMap().size()).isEqualTo(5);

    database.transaction(() -> {
      Assertions.assertThat(database.getSchema().existsType("V")).isTrue();
      database.getSchema().getDictionary().updateName("name", "firstName");
    });

    assertThat(database.getSchema().getDictionary().getDictionaryMap().size()).isEqualTo(5);

    database.transaction(() -> {
      final ResultSet iter = database.query("sql", "select from V order by id asc");

      int i = 0;
      while (iter.hasNext()) {
        final Document d = (Document) iter.next().getRecord().get();

        Assertions.assertThat(d.getInteger("id")).isEqualTo(i);
        Assertions.assertThat(d.getString("firstName")).isEqualTo("Jay");
        Assertions.assertThat(d.getString("surname")).isEqualTo("Miner");

        if (i == 10)
          Assertions.assertThat(d.getString("newProperty")).isEqualTo("newProperty");
        else
          Assertions.assertThat(d.getString("newProperty")).isNull();

        Assertions.assertThat(d.getString("name")).isNull();

        ++i;
      }

      Assertions.assertThat(i).isEqualTo(11);
    });

    try {
      database.transaction(() -> {
        Assertions.assertThat(database.getSchema().existsType("V")).isTrue();
        database.getSchema().getDictionary().updateName("V", "V2");
      });
      fail("");
    } catch (final Exception e) {
    }
  }

  @Test
  public void namesClash() {
    database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      final int finalI = i;
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Babylonia");
        for (int k = 0; k < 10; k++) {
          v.set("origin", finalI);
          v.set("p" + ((finalI * 10) + k), ((finalI * 10) + k));
        }
        v.save();
      });
    }

    for (final Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      assertThat(v.getPropertyNames().size()).isEqualTo(11);

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        assertThat(value).isEqualTo((origin * 10) + k);
      }
    }
  }

  @Test
  public void namesClashPropertyCreatedOnSchemaBefore() {
    final VertexType babylonia = database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      final int finalI = i;

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

    for (final Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      assertThat(v.getPropertyNames().size()).isEqualTo(11);

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        assertThat(value).isEqualTo((origin * 10) + k);
      }
    }
  }

  @Test
  public void namesClashPropertyCreatedOnSchemaSameTx() {
    final VertexType babylonia = database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      final int finalI = i;

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

    for (final Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      assertThat(v.getPropertyNames().size()).isEqualTo(11);

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        assertThat(value).isEqualTo((origin * 10) + k);
      }
    }
  }

  @Test
  public void namesClashPropertyCreatedOnSchemaSubTx() {
    final VertexType babylonia = database.getSchema().getOrCreateVertexType("Babylonia");

    for (int i = 0; i < 10; i++) {
      final int finalI = i;

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

    for (final Iterator<Record> iterator = database.iterateType("Babylonia", true); iterator.hasNext(); ) {
      final Vertex v = iterator.next().asVertex();
      assertThat(v.getPropertyNames().size()).isEqualTo(11);

      final int origin = v.getInteger("origin");

      for (int k = 0; k < 10; k++) {
        final Integer value = v.getInteger("p" + ((origin * 10) + k));
        assertThat(value).isEqualTo((origin * 10) + k);
      }
    }
  }
}
