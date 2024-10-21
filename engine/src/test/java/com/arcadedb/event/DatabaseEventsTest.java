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
package com.arcadedb.event;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TODO: ADD TESTS FOR DOCUMENTS AND EDGES
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseEventsTest extends TestHelper {
  @Override
  public void beginTest() {
    database.getSchema().createVertexType("Vertex");
    database.getSchema().createEdgeType("Edge");
    database.getSchema().createDocumentType("Document");
  }

  @Test
  public void testBeforeCreate() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordCreateListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(1);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2");
        assertThat(counter.get()).isEqualTo(1);
        v2.save();
        assertThat(counter.get()).isEqualTo(2);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterCreate() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordCreateListener listener = record -> counter.incrementAndGet();

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(1);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2");
        assertThat(counter.get()).isEqualTo(1);
        v2.save();
        assertThat(counter.get()).isEqualTo(2);
        assertThat(database.countType("Vertex", true)).isEqualTo(2);
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testBeforeRead() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordReadListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.set("modified2", true);
        assertThat(counter.get()).isEqualTo(1);
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testBeforeUpdate() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordUpdateListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);

        v1.set("modified", true);
        assertThat(counter.get()).isEqualTo(0);

        v1.save();
        assertThat(counter.get()).isEqualTo(1);
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.set("modified2", true);
        assertThat(counter.get()).isEqualTo(1);

        v1.save();
        assertThat(counter.get()).isEqualTo(2);
      });

      assertThat(database.iterateType("Vertex", true).next().asVertex().has("modified2")).isFalse();

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterRead() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordReadListener listener = record -> {
      counter.incrementAndGet();
      return record;
    };

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.set("modified2", true);
        assertThat(counter.get()).isEqualTo(1);
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterUpdate() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordUpdateListener listener = record -> counter.incrementAndGet();

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);

        v1.set("modified", true);
        assertThat(counter.get()).isEqualTo(0);

        v1.save();
        assertThat(counter.get()).isEqualTo(1);
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testBeforeDelete() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordDeleteListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);

        v1.set("modified", true);
        assertThat(counter.get()).isEqualTo(0);

        v1.save();
        assertThat(counter.get()).isEqualTo(0);
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.delete();
        assertThat(counter.get()).isEqualTo(1);

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2").save();
        v2.delete();
        assertThat(counter.get()).isEqualTo(2);
      });

      assertThat(database.countType("Vertex", true)).isEqualTo(1);

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterDelete() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordDeleteListener listener = record -> counter.incrementAndGet();

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        assertThat(counter.get()).isEqualTo(0);
        v1.save();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(database.countType("Vertex", true)).isEqualTo(1);

        v1.set("modified", true);
        assertThat(counter.get()).isEqualTo(0);

        v1.save();
        assertThat(counter.get()).isEqualTo(0);
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.delete();
        assertThat(counter.get()).isEqualTo(1);

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2").save();
        v2.delete();
        assertThat(counter.get()).isEqualTo(2);
      });

      assertThat(database.countType("Vertex", true)).isEqualTo(0);

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  // Issue https://github.com/ArcadeData/arcadedb/issues/807
  @Test
  public void testBeforeCreateEmulateIncrement() {
    final BeforeRecordCreateListener listener = record -> {
      int maxId = database.query("SQL", "select max(counter) from `IndexedVertex`").nextIfAvailable().getProperty("max(counter)", 0);
      record.asVertex().modify().set("counter", maxId + 1);
      return true;
    };

    database.getEvents().registerListener(listener);
    try {
      database.getSchema().createVertexType("IndexedVertex").createProperty("counter", Type.INTEGER).createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("IndexedVertex").set("id", "test");
        assertThat(v1.has("counter")).isFalse();
        v1.save();
        assertThat(v1.get("counter")).isEqualTo(1);
        assertThat(database.countType("IndexedVertex", true)).isEqualTo(1);

        // SHOULD OVERWRITE THIS
        database.newVertex("IndexedVertex").set("id", "test2").set("counter", 1).save();

        final MutableVertex v2 = database.newVertex("IndexedVertex").set("id", "test3");
        assertThat(v2.has("counter")).isFalse();
        v2.save();
        assertThat(v2.get("counter")).isEqualTo(3);
        assertThat(database.countType("IndexedVertex", true)).isEqualTo(3);

        assertThat(database.query("SQL", "select from `IndexedVertex` where counter= 2").nextIfAvailable().<String>getProperty("id")).isEqualTo("test2");
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }
}
