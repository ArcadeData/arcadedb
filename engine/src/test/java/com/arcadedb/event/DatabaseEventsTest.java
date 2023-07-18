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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(1, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2");
        Assertions.assertEquals(1, counter.get());
        v2.save();
        Assertions.assertEquals(2, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));
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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(1, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2");
        Assertions.assertEquals(1, counter.get());
        v2.save();
        Assertions.assertEquals(2, counter.get());
        Assertions.assertEquals(2, database.countType("Vertex", true));
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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(0, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.set("modified2", true);
        Assertions.assertEquals(1, counter.get());
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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(0, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        v1.set("modified", true);
        Assertions.assertEquals(0, counter.get());

        v1.save();
        Assertions.assertEquals(1, counter.get());
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.set("modified2", true);
        Assertions.assertEquals(1, counter.get());

        v1.save();
        Assertions.assertEquals(2, counter.get());
      });

      Assertions.assertFalse(database.iterateType("Vertex", true).next().asVertex().has("modified2"));

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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(0, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.set("modified2", true);
        Assertions.assertEquals(1, counter.get());
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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(0, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        v1.set("modified", true);
        Assertions.assertEquals(0, counter.get());

        v1.save();
        Assertions.assertEquals(1, counter.get());
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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(0, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        v1.set("modified", true);
        Assertions.assertEquals(0, counter.get());

        v1.save();
        Assertions.assertEquals(0, counter.get());
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.delete();
        Assertions.assertEquals(1, counter.get());

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2").save();
        v2.delete();
        Assertions.assertEquals(2, counter.get());
      });

      Assertions.assertEquals(1, database.countType("Vertex", true));

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
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(0, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        v1.set("modified", true);
        Assertions.assertEquals(0, counter.get());

        v1.save();
        Assertions.assertEquals(0, counter.get());
      });

      database.transaction(() -> {
        final MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.delete();
        Assertions.assertEquals(1, counter.get());

        final MutableVertex v2 = database.newVertex("Vertex").set("id", "test2").save();
        v2.delete();
        Assertions.assertEquals(2, counter.get());
      });

      Assertions.assertEquals(0, database.countType("Vertex", true));

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
        Assertions.assertFalse(v1.has("counter"));
        v1.save();
        Assertions.assertEquals(1, v1.get("counter"));
        Assertions.assertEquals(1, database.countType("IndexedVertex", true));

        // SHOULD OVERWRITE THIS
        database.newVertex("IndexedVertex").set("id", "test2").set("counter", 1).save();

        final MutableVertex v2 = database.newVertex("IndexedVertex").set("id", "test3");
        Assertions.assertFalse(v2.has("counter"));
        v2.save();
        Assertions.assertEquals(3, v2.get("counter"));
        Assertions.assertEquals(3, database.countType("IndexedVertex", true));

        Assertions.assertEquals("test2", database.query("SQL", "select from `IndexedVertex` where counter= 2").nextIfAvailable().getProperty("id"));
      });

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }
}
