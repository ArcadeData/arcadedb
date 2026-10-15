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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10107: three production paths append a batch to a time series engine - the line protocol and gRPC
 * ({@code TimeSeriesGateway.write}), the Prometheus remote write and the JSONL importer - and the third handed the engine the
 * raw values of its source, so a typed tag written as text aborted the whole import. The conversion had been added to the
 * other two call sites one by one (issues #9809, #9972) and then moved into {@link TimeSeriesStorableRows} (issue #10030), which
 * is now the ONE way a client-facing path reaches {@code TimeSeriesEngine.appendBatch(long[], Object[][])}:
 * {@link TimeSeriesStorableRows#convert} builds the grid, {@link TimeSeriesStorableRows#appendTo} appends it.
 * <p>
 * This is the census of the callers, over the production sources of every module in the shape of {@code LogMessageArityTest}:
 * a fourth endpoint calling {@code appendBatch} with a grid of its own fails here and is sent to the converting entry point,
 * instead of being found by the next user whose file holds a quoted number. Each path is exercised with a text value for a typed
 * column by its own test: {@code Issue9809LineProtocolTemporalValueTest} (line protocol), {@code Issue9972PrometheusTypedTagIT}
 * (Prometheus remote write) and {@code Issue10107JsonlTimeSeriesUnstorableValueTest} (JSONL import).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10107AppendBatchCallersConvertTest {
  private static final Pattern APPEND_BATCH_CALL = Pattern.compile("\\.\\s*appendBatch\\s*\\(");

  /**
   * The engine itself, the class that appends the converted grid, and the javadoc example of the row-source form (a
   * {@code TimeSeriesBatch} the caller fills with typed values, which no client-facing path uses).
   */
  private static final Set<String> ALLOWED = Set.of( //
      "engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesEngine.java",
      "engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesStorableRows.java",
      "engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesBatch.java");

  @Test
  void noProductionPathAppendsAGridOfItsOwn() throws IOException {
    final Path repoRoot = Path.of("").toAbsolutePath().getParent();
    assertThat(repoRoot).isNotNull();

    final List<String> offenders = new ArrayList<>();
    int scanned = 0;
    try (final Stream<Path> modules = Files.list(repoRoot)) {
      for (final Path module : modules.sorted().toList()) {
        final Path sources = module.resolve("src").resolve("main").resolve("java");
        if (!Files.isDirectory(sources))
          continue;

        try (final Stream<Path> files = Files.walk(sources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
            scanned++;
            final String relative = repoRoot.relativize(file).toString().replace('\\', '/');
            if (!ALLOWED.contains(relative) && APPEND_BATCH_CALL.matcher(read(file)).find())
              offenders.add(relative);
          }
        }
      }
    }

    assertThat(scanned).as("no production source found under " + repoRoot).isGreaterThan(1000);
    assertThat(offenders)
        .as("production classes that call TimeSeriesEngine.appendBatch with a grid of their own, bypassing the conversion every "
            + "client-facing ingestion path shares: build the grid with TimeSeriesStorableRows.convert and append it with "
            + "TimeSeriesStorableRows.appendTo (issue #10107)")
        .isEmpty();
  }

  private static String read(final Path file) {
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
