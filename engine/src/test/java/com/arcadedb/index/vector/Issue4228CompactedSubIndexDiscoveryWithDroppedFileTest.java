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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4228: at server startup, {@code LSMVectorIndex.discoverAndLoadCompactedSubIndex()}
 * iterates {@code FileManager.getFiles()} which is a sparse list (dropped slots are {@code null}).
 * Before the fix it dereferenced the loop variable without a null check, threw NPE, the outer
 * {@code catch (Exception)} swallowed it and returned {@code null}. An existing compacted file was
 * therefore orphaned for the lifetime of the process, silently degrading kNN search performance.
 * <p>
 * The sibling discovery method {@code discoverAndLoadGraphFile()} already null-checks the loop
 * variable correctly, so the fix mirrors that pattern.
 * <p>
 * This test does not need to trigger a real compaction. The bug fires purely from iterating a
 * sparse file list, so it is enough to:
 * <ol>
 *   <li>create a filler type whose bucket grabs an early {@code fileId} slot;</li>
 *   <li>create a vector index;</li>
 *   <li>drop the filler type so its bucket file is removed from disk - on the next reopen
 *       {@link com.arcadedb.engine.FileManager} will scan only the surviving files and leave the
 *       dropped {@code fileId} slot as {@code null};</li>
 *   <li>reopen the database while capturing the log: before the fix this emits an error message
 *       complaining about a {@code null} {@link ComponentFile}; after the fix the loop skips the
 *       {@code null} slot and stays silent.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4228CompactedSubIndexDiscoveryWithDroppedFileTest extends TestHelper {

  private static final int DIMENSIONS   = 16;
  private static final int VECTOR_COUNT = 32;

  @Test
  void discoveryShouldNotNpeOnNullFileSlotAfterReopen() throws Exception {
    // 1) Filler type whose bucket grabs an early fileId. Dropping it later produces a null slot in
    //    FileManager that survives the reopen because the bucket file is physically deleted.
    database.transaction(() -> {
      final DocumentType filler = database.getSchema().createDocumentType("Filler");
      filler.createProperty("name", Type.STRING);
    });

    // 2) Vector index + a handful of vectors. We do not need a compacted file to exist on disk:
    //    the bug fires from the iteration itself, on the very first null slot encountered, before
    //    any compacted-file matching logic runs.
    database.transaction(() -> {
      final DocumentType embedding = database.getSchema().createDocumentType("Embedding");
      embedding.createProperty("name", Type.STRING);
      embedding.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA {
            "dimensions": %d,
            "similarity": "COSINE",
            "maxConnections": 16,
            "beamWidth": 100,
            "idPropertyName": "name"
          }""".formatted(DIMENSIONS));
    });

    database.transaction(() -> {
      for (int i = 0; i < VECTOR_COUNT; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = i * 0.01f + j * 0.001f;
        database.newDocument("Embedding").set("name", "v" + i).set("vector", vector).save();
      }
    });

    // 3) Drop the filler type: its bucket file is removed from disk and its FileManager slot will
    //    be null after reopen.
    database.getSchema().dropType("Filler");

    // 4) Sanity: at least one slot in FileManager is currently null (the dropped filler bucket).
    assertThat(hasNullFileSlot((DatabaseInternal) database))
        .as("After dropping Filler, FileManager.getFiles() must contain a null slot")
        .isTrue();

    // 5) Capture warnings/severe logs during the reopen. Before the fix the iteration NPE is caught
    //    generically and logged with the unmistakable message produced by ComponentFile.getComponentName()
    //    on a null receiver.
    final List<String> captured = new CopyOnWriteArrayList<>();
    final Logger originalLogger = readField(LogManager.instance(), "logger");
    LogManager.instance().setLogger(new CapturingLogger(captured, originalLogger));

    try {
      // The bug surfaces during the reopen: LSMVectorIndex(load) constructor calls
      // discoverAndLoadCompactedSubIndex() which iterates FileManager.getFiles().
      reopenDatabase();

      // FileManager must still have a null slot after reopen for the test to be meaningful: the
      // dropped fileId is persistent because it is encoded in the surviving files' names.
      assertThat(hasNullFileSlot((DatabaseInternal) database))
          .as("After reopen FileManager.getFiles() must still expose the null slot - otherwise the "
              + "test is not actually exercising the buggy code path")
          .isTrue();

      final List<String> matching = captured.stream()
          .filter(m -> m != null && m.contains("Error discovering compacted sub-index"))
          .toList();

      assertThat(matching)
          .as("The discovery loop must not throw NPE on null slots in FileManager.getFiles(). "
              + "Captured logs at WARNING+ during reopen: %s", captured)
          .isEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  private static boolean hasNullFileSlot(final DatabaseInternal database) {
    for (final ComponentFile f : database.getFileManager().getFiles()) {
      if (f == null)
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(final Object target, final String name) throws Exception {
    final Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return (T) f.get(target);
  }

  /**
   * Captures messages at WARNING and above. Forwards every record to the production logger so test
   * output still shows what fired. Pattern lifted from {@code LocalSchemaOrphanIndexSelfHealTest}.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> captured;
    private final Logger       delegate;

    CapturingLogger(final List<String> captured, final Logger delegate) {
      this.captured = captured;
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
          // Fall back to the raw template; good enough for substring matching.
        }
      }
      captured.add(formatted);
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
