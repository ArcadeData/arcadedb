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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

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
        MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(1, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        MutableVertex v2 = database.newVertex("Vertex").set("id", "test2");
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
        MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
        Assertions.assertEquals(0, counter.get());
        v1.save();
        Assertions.assertEquals(1, counter.get());
        Assertions.assertEquals(1, database.countType("Vertex", true));

        MutableVertex v2 = database.newVertex("Vertex").set("id", "test2");
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
  public void testBeforeUpdate() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordUpdateListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
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
        MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
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
  public void testAfterUpdate() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordUpdateListener listener = record -> counter.incrementAndGet();

    database.getEvents().registerListener(listener);
    try {

      database.transaction(() -> {
        MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
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
        MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
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
        MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.delete();
        Assertions.assertEquals(1, counter.get());

        MutableVertex v2 = database.newVertex("Vertex").set("id", "test2").save();
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
        MutableVertex v1 = database.newVertex("Vertex").set("id", "test");
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
        MutableVertex v1 = database.iterateType("Vertex", true).next().asVertex().modify();
        v1.delete();
        Assertions.assertEquals(1, counter.get());

        MutableVertex v2 = database.newVertex("Vertex").set("id", "test2").save();
        v2.delete();
        Assertions.assertEquals(2, counter.get());
      });

      Assertions.assertEquals(0, database.countType("Vertex", true));

    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }
}
