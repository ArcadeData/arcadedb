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
package com.arcadedb.engine.timeseries;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10107: three production paths append a batch to a time series engine - the line protocol and gRPC
 * ({@code TimeSeriesGateway.write}), the Prometheus remote write and the JSONL importer - and the third handed the engine the
 * raw values of its source, so a typed tag written as text aborted the whole import. The conversion had been added to the
 * other two call sites one by one (issues #9809, #9972) and then moved into {@link TimeSeriesStorableRows} (issue #10030), which
 * is now the ONE way a client-facing path reaches {@code TimeSeriesEngine.appendBatch(long[], Object[][])}:
 * {@link TimeSeriesStorableRows#convert} builds the grid, {@link TimeSeriesStorableRows#appendTo} appends it.
 * <p>
 * Issue #10381 found a fourth, the embedded async API, which reached the same storage through {@code newRowSource} and
 * {@code shard.appendSamples} - a pattern this census did not look for - and the census walked the modules one level deep, so a
 * nested module such as {@code bindings/python} was never scanned. It now looks for EVERY entry point into the engine's storage
 * ({@code appendBatch}, {@code appendSamples}, {@code newRowSource}) and walks every {@code src/main/java} of the repository.
 * <p>
 * This is the census of the callers, over the production sources of every module in the shape of {@code LogMessageArityTest}:
 * a new endpoint calling one of them with a grid of its own fails here and is sent to the converting entry points, instead of
 * being found by the next user whose file holds a quoted number or whose column declares a MAX. Each path is exercised with a text
 * value for a typed column by its own test: {@code Issue9809LineProtocolTemporalValueTest} (line protocol),
 * {@code Issue9972PrometheusTypedTagIT} (Prometheus remote write), {@code Issue10107JsonlTimeSeriesUnstorableValueTest} (JSONL
 * import) and {@code Issue10381AsyncAppendSamplesConstraintsTest} (the async API).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10107AppendBatchCallersConvertTest {
  private static final Pattern ENTRY_POINT = Pattern.compile("\\.\\s*(appendBatch|appendSamples|newRowSource)\\s*\\(");

  /** Directories that hold no production source of the repository, however deep. */
  private static final Set<String> NOT_SOURCE = Set.of("node_modules", "target", ".git", ".claude", ".idea");

  /**
   * The engine itself, the class that appends the converted grid, and the two classes that reach the storage with a source they
   * hold to the same rules themselves: the SQL step converts and checks the one row of the statement
   * ({@code SaveElementStep.saveToTimeSeries}), and the async task appends a source the executor converted and checked on the
   * calling thread before scheduling it ({@code TimeSeriesStorableRows.requireStorable}, either overload).
   */
  private static final Set<String> ALLOWED = Set.of( //
      "engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesEngine.java",
      "engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesShard.java",
      "engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesStorableRows.java",
      "engine/src/main/java/com/arcadedb/query/sql/executor/SaveElementStep.java",
      "engine/src/main/java/com/arcadedb/database/async/DatabaseAsyncAppendSamples.java");

  @Test
  void noProductionPathAppendsAGridOfItsOwn() throws IOException {
    final Path repoRoot = Path.of("").toAbsolutePath().getParent();
    assertThat(repoRoot).isNotNull();

    final List<String> offenders = new ArrayList<>();
    final int[] scanned = { 0 };
    Files.walkFileTree(repoRoot, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
        return NOT_SOURCE.contains(String.valueOf(dir.getFileName())) ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
        final String relative = repoRoot.relativize(file).toString().replace('\\', '/');
        if (!relative.endsWith(".java") || !(relative.startsWith("src/main/java/") || relative.contains("/src/main/java/")))
          return FileVisitResult.CONTINUE;

        scanned[0]++;
        if (!ALLOWED.contains(relative) && callsAnEntryPoint(file))
          offenders.add(relative);
        return FileVisitResult.CONTINUE;
      }
    });

    assertThat(scanned[0]).as("no production source found under " + repoRoot).isGreaterThan(1000);
    assertThat(offenders)
        .as("production classes that reach the storage of a time series engine (appendBatch, appendSamples, newRowSource) with a "
            + "grid of their own, bypassing the conversion and the column constraints every ingestion path shares: build the rows "
            + "with TimeSeriesStorableRows (convert + appendTo, or requireStorable for the async API) "
            + "(issues #10107, #10381)")
        .isEmpty();
  }

  /** Comment lines are not calls: the javadoc of the engine names these methods all over. */
  private static boolean callsAnEntryPoint(final Path file) {
    try {
      for (final String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
        final String code = line.stripLeading();
        if (!code.startsWith("//") && !code.startsWith("*") && !code.startsWith("/*") && ENTRY_POINT.matcher(code).find())
          return true;
      }
      return false;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
