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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4063. When the schema configuration on disk references an index
 * by an older file name than the one currently loaded in memory (the typical outcome of an
 * LSM compaction that renames the mutable file via {@code System.nanoTime()}), the orphan
 * relinking pass in {@link LocalSchema#readConfiguration()} must reattach the index by bucket
 * prefix without logging a misleading "Cannot find index" warning at the WARNING level.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LocalSchemaOrphanIndexLoadTest extends TestHelper {

  @Test
  void renamedIndexFileGetsReattachedSilently() throws Exception {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Item");
      type.createProperty("code", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "code");
    });

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Item").set("code", "code-" + i).save();
    });

    // Capture the current index name then mutate the on-disk schema configuration to reference
    // an older synthetic name with the same bucket prefix. This mirrors the situation produced
    // by an LSM compaction that renames the mutable file: {@code mutable.getName()} returns
    // the new name, but the JSON written before the rename still points to the previous one.
    final File schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile();
    final String original = FileUtils.readFileAsString(schemaFile);
    final JSONObject schemaJson = new JSONObject(original);
    final JSONObject typeJson = schemaJson.getJSONObject("types").getJSONObject("Item");
    final JSONObject indexes = typeJson.getJSONObject("indexes");
    final List<String> originalNames = new ArrayList<>(indexes.keySet());
    assertThat(originalNames).isNotEmpty();

    final String firstName = originalNames.get(0);
    final int last = firstName.lastIndexOf('_');
    final String bucketPrefix = firstName.substring(0, last);
    final String renamedName = bucketPrefix + "_" + (System.nanoTime() + 1);

    final JSONObject indexBody = indexes.getJSONObject(firstName);
    indexes.remove(firstName);
    indexes.put(renamedName, indexBody);

    try (final java.io.FileWriter w = new java.io.FileWriter(schemaFile)) {
      w.write(schemaJson.toString());
    }

    final CapturingHandler handler = new CapturingHandler();
    handler.setLevel(Level.ALL);
    final Logger logger = Logger.getLogger(LocalSchema.class.getName());
    logger.addHandler(handler);
    final Level prev = logger.getLevel();
    logger.setLevel(Level.ALL);
    try {
      database.close();
      database = factory.open();

      assertThat(database.getSchema().getType("Item")).isNotNull();
      assertThat(database.countType("Item", false)).isEqualTo(10);
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(prev);
    }

    final List<String> warningsAboutMissingIndexes = handler.snapshot().stream()
        .filter(m -> m.contains("Cannot find index") || m.contains("Cannot find indexes"))
        .toList();
    assertThat(warningsAboutMissingIndexes)
        .as("Renaming an index file (LSM compaction) must not surface a 'Cannot find index' "
            + "warning when the orphan relinker can reattach by bucket prefix; got: %s",
            warningsAboutMissingIndexes)
        .isEmpty();
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/LocalSchemaOrphanIndexLoadTest";
  }

  private static final class CapturingHandler extends Handler {
    private final List<String> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      if (record == null || record.getLevel().intValue() < Level.WARNING.intValue())
        return;
      String msg = record.getMessage();
      if (msg != null && record.getParameters() != null && record.getParameters().length > 0) {
        try {
          msg = msg.formatted(record.getParameters());
        } catch (final Exception ignored) {
        }
      }
      if (msg != null)
        records.add(msg);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws SecurityException {
    }

    List<String> snapshot() {
      return new ArrayList<>(records);
    }
  }
}
