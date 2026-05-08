/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the user-reported follow-up to issue #4083: on an HA follower whose
 * persisted schema.json carries an index reference whose backing file is genuinely missing
 * (the orphan-relinker has nothing to bind it to), the previous behaviour kept the dangling
 * reference on disk and re-emitted the same {@code "Cannot find indexes [...]"} warning on
 * every subsequent schema load. mdre's screenshot in the issue shows the warning repeating
 * across many types because {@code applySchemaEntry} runs {@code load()} on every SCHEMA_ENTRY
 * apply.
 * <p>
 * The fix marks the schema dirty when a warning is emitted, so the next save rewrites
 * schema.json without the dangling reference and the warning self-heals.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LocalSchemaOrphanIndexSelfHealTest extends TestHelper {

  @Test
  void unrelinkableOrphanIndexReferenceIsScrubbedFromSchemaJsonOnLoad() throws IOException {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Widget");
      type.createProperty("code", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Widget", "code");
    });

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Widget").set("code", "code-" + i).save();
    });

    // Inject a schema reference whose bucket prefix does not match any bucket on disk - the
    // orphan-relinker walks bucketMap by prefix, so a synthetic prefix that cannot be resolved
    // is exactly the case that produces "Cannot find indexes [...]" on a real follower.
    final File schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile();
    final String original = FileUtils.readFileAsString(schemaFile);
    final JSONObject schemaJson = new JSONObject(original);
    final JSONObject typeJson = schemaJson.getJSONObject("types").getJSONObject("Widget");
    final JSONObject indexes = typeJson.getJSONObject("indexes");

    // Add a synthetic dangling reference. Use a bucket name that does NOT exist anywhere in this
    // database (NoSuchBucket_99), so neither exact name match nor bucket-prefix relinking can
    // possibly bind it to a file. This is the worst-case mdre observed on his followers.
    final String danglingName = "NoSuchBucket_99_" + System.nanoTime();
    final JSONObject sample = indexes.getJSONObject(indexes.keySet().iterator().next());
    final JSONObject danglingBody = new JSONObject(sample.toString());
    indexes.put(danglingName, danglingBody);

    try (final java.io.FileWriter w = new java.io.FileWriter(schemaFile)) {
      w.write(schemaJson.toString());
    }

    final CapturingHandler firstHandler = new CapturingHandler();
    firstHandler.setLevel(Level.WARNING);
    final Logger logger = Logger.getLogger(LocalSchema.class.getName());
    logger.addHandler(firstHandler);
    final Level prev = logger.getLevel();
    logger.setLevel(Level.ALL);

    try {
      // First reopen: the warning fires, but the new self-heal path marks the schema dirty so
      // saveConfiguration() rewrites schema.json without the dangling reference.
      database.close();
      database = factory.open();

      final List<String> firstWarnings = firstHandler.snapshot().stream()
          .filter(m -> m.contains("Cannot find indexes") || m.contains("Cannot find index"))
          .toList();
      assertThat(firstWarnings)
          .as("first load must surface the 'Cannot find indexes' warning so the operator notices the divergence")
          .isNotEmpty();

      // The on-disk schema must no longer contain the dangling reference - that is the whole
      // point of the self-heal, and what makes the warning stop repeating on every subsequent
      // SCHEMA_ENTRY apply.
      database.close();
      final String afterHeal = FileUtils.readFileAsString(schemaFile);
      final JSONObject healedJson = new JSONObject(afterHeal);
      final JSONObject healedIndexes = healedJson.getJSONObject("types").getJSONObject("Widget")
          .getJSONObject("indexes");
      assertThat(healedIndexes.has(danglingName))
          .as("the dangling index reference must be scrubbed from schema.json after the warning fires")
          .isFalse();

      // Second reopen with a fresh handler: the dangling reference is gone, so no warning at all.
      logger.removeHandler(firstHandler);
      final CapturingHandler secondHandler = new CapturingHandler();
      secondHandler.setLevel(Level.WARNING);
      logger.addHandler(secondHandler);
      try {
        database = factory.open();

        final List<String> secondWarnings = secondHandler.snapshot().stream()
            .filter(m -> m.contains("Cannot find indexes") || m.contains("Cannot find index"))
            .toList();
        assertThat(secondWarnings)
            .as("second load must NOT repeat the warning - the self-heal pass already removed the dangling reference")
            .isEmpty();
      } finally {
        logger.removeHandler(secondHandler);
      }
    } finally {
      logger.setLevel(prev);
    }
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/LocalSchemaOrphanIndexSelfHealTest";
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
