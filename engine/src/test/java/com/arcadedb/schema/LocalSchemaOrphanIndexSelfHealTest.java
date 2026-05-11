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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

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
  void unrelinkableOrphanIndexReferenceIsScrubbedFromSchemaJsonOnLoad() throws Exception {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Widget");
      type.createProperty("code", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Widget", "code");
    });

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Widget").set("code", "code-" + i).save();
    });

    // Resolve the schema file location while the database is still open, then close it. The
    // schema.json must be edited AFTER close() because {@link LocalSchema#close()} will rewrite
    // the file when {@code dirtyGeneration > savedGeneration} - in some test orderings the
    // schema is left dirty by the just-finished writes and the in-memory state (without the
    // injected dangling reference) would otherwise overwrite our edit before the next open().
    final File schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile();
    database.close();

    // Inject a schema reference whose bucket prefix does not match any bucket on disk - the
    // orphan-relinker walks bucketMap by prefix, so a synthetic prefix that cannot be resolved
    // is exactly the case that produces "Cannot find indexes [...]" on a real follower.
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

    // Capture warnings by swapping the active LogManager logger for the duration of the test.
    // This bypasses JUL entirely, so the assertion does not depend on JUL configuration state
    // left over by earlier tests in the same JVM (logger levels, filters, handler chains).
    // The pattern mirrors QueryEngineManagerPoolTest.saturationLogsThrottledWarning.
    final List<String> warnings = new CopyOnWriteArrayList<>();
    final Logger originalLogger = readField(LogManager.instance(), "logger");
    LogManager.instance().setLogger(new CapturingLogger(warnings, originalLogger));

    try {
      // First reopen: the warning fires, but the new self-heal path marks the schema dirty so
      // saveConfiguration() rewrites schema.json without the dangling reference.
      database = factory.open();

      final List<String> firstWarnings = warnings.stream()
          .filter(m -> m.contains("Cannot find indexes") || m.contains("Cannot find index"))
          .toList();
      assertThat(firstWarnings)
          .as("first load must surface the 'Cannot find indexes' warning so the operator notices the divergence (captured=%s)",
              warnings)
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

      // Second reopen: the dangling reference is gone, so no warning at all.
      warnings.clear();
      database = factory.open();

      final List<String> secondWarnings = warnings.stream()
          .filter(m -> m.contains("Cannot find indexes") || m.contains("Cannot find index"))
          .toList();
      assertThat(secondWarnings)
          .as("second load must NOT repeat the warning - the self-heal pass already removed the dangling reference")
          .isEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/LocalSchemaOrphanIndexSelfHealTest";
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(final Object target, final String name) throws Exception {
    final Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return (T) f.get(target);
  }

  /**
   * Captures WARNING-and-above messages into a list while forwarding every record to the
   * production logger so the test output still shows what fired.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;
    private final Logger       delegate;

    CapturingLogger(final List<String> warnings, final Logger delegate) {
      this.warnings = warnings;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template - good enough for our substring matching below.
        }
      }
      warnings.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
          arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
          arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
