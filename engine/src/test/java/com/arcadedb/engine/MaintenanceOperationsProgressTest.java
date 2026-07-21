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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * REBUILD INDEX and COMPACT INDEX publish their progress in {@link OperationProgressRegistry} (issue #5376),
 * exactly like CHECK DATABASE: registered while running, retired in a finally - success or failure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MaintenanceOperationsProgressTest extends TestHelper {

  private void createIndexedType(final int records) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Doc", "id");
    });
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument("Doc").set("id", i).save();
    });
  }

  /** Samples the registry from a background thread while the synchronous statement runs. */
  private List<OperationProgress> sampleWhile(final Runnable command) throws InterruptedException {
    final List<OperationProgress> samples = new ArrayList<>();
    final AtomicBoolean running = new AtomicBoolean(true);
    final Thread sampler = new Thread(() -> {
      while (running.get()) {
        samples.addAll(OperationProgressRegistry.instance().getOperations(database.getName()));
        try {
          // 1ms cadence still catches the multi-second rebuild reliably without busy-spinning a CI core.
          Thread.sleep(1);
        } catch (final InterruptedException e) {
          return;
        }
      }
    });
    sampler.setDaemon(true);
    sampler.start();
    try {
      command.run();
    } finally {
      running.set(false);
      sampler.join(2_000);
    }
    return samples;
  }

  @Test
  @Tag("slow")
  void rebuildIndexPublishesAndRetiresProgress() throws InterruptedException {
    createIndexedType(100_000);

    final List<OperationProgress> samples = sampleWhile(() ->
        database.command("sql", "REBUILD INDEX *").close());

    // THE OPERATION WAS VISIBLE WHILE RUNNING...
    assertThat(samples).as("the rebuild must be visible in the registry while it runs").isNotEmpty();
    assertThat(samples.stream().anyMatch(op -> op.getOperation().equals("rebuild index"))).isTrue();
    assertThat(samples.stream().anyMatch(op -> op.getStepName().startsWith("Rebuilding index"))).isTrue();
    // ...AND RETIRED WHEN THE STATEMENT COMPLETED.
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  @Test
  void compactIndexRetiresProgress() {
    createIndexedType(1_000);

    database.command("sql", "COMPACT INDEX *").close();

    // COMPACTION IS FAST, SO ONLY THE RETIRE CONTRACT IS ASSERTED DETERMINISTICALLY.
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  @Test
  void failedRebuildRetiresProgressToo() {
    createIndexedType(10);

    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `does_not_exist`").close())
        .isInstanceOf(Exception.class);

    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }
}
