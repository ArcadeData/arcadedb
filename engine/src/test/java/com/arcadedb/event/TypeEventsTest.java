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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.*;

import org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeEventsTest extends TestHelper {
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

    database.getSchema().getType("Vertex").getEvents().registerListener(listener);
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
      database.getSchema().getType("Vertex").getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterCreate() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordCreateListener listener = record -> counter.incrementAndGet();

    database.getSchema().getType("Vertex").getEvents().registerListener(listener);
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
      database.getSchema().getType("Vertex").getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testBeforeUpdate() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordUpdateListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getSchema().getType("Vertex").getEvents().registerListener(listener);
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
      database.getSchema().getType("Vertex").getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterUpdate() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordUpdateListener listener = record -> counter.incrementAndGet();

    database.getSchema().getType("Vertex").getEvents().registerListener(listener);
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
      database.getSchema().getType("Vertex").getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testBeforeDelete() {
    final AtomicInteger counter = new AtomicInteger();
    final BeforeRecordDeleteListener listener = record -> {
      counter.incrementAndGet();
      return counter.get() == 1;
    };

    database.getSchema().getType("Vertex").getEvents().registerListener(listener);
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
      database.getSchema().getType("Vertex").getEvents().unregisterListener(listener);
    }
  }

  @Test
  public void testAfterDelete() {
    final AtomicInteger counter = new AtomicInteger();
    final AfterRecordDeleteListener listener = record -> counter.incrementAndGet();

    database.getSchema().getType("Vertex").getEvents().registerListener(listener);
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
      database.getSchema().getType("Vertex").getEvents().unregisterListener(listener);
    }
  }
}
