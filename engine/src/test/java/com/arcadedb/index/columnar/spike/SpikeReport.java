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
package com.arcadedb.index.columnar.spike;

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;

/**
 * Spike-only report: collects Markdown tables, logs every line and writes target/columnar-spike/results.md.
 */
final class SpikeReport {
  static final Path DIRECTORY = Path.of("target", "columnar-spike");

  private final StringBuilder out         = new StringBuilder();
  private       boolean       metricTable = false;

  void row(final String metric, final String value, final String note) {
    if (!metricTable) {
      line("| metric | value | note |");
      line("|---|---|---|");
      metricTable = true;
    }
    line("| " + cell(metric) + " | " + cell(value) + " | " + cell(note) + " |");
  }

  void table(final String title, final String[] header, final List<String[]> rows) {
    metricTable = false;
    line("");
    line("### " + title);
    line("");
    line("| " + String.join(" | ", header) + " |");
    line("|" + "---|".repeat(header.length));
    for (final String[] row : rows) {
      final String[] cells = new String[row.length];
      for (int i = 0; i < row.length; i++)
        cells[i] = cell(row[i]);
      line("| " + String.join(" | ", cells) + " |");
    }
    line("");
  }

  /** Free text line (heading, plan text, verdict). */
  void text(final String text) {
    metricTable = false;
    for (final String l : text.split("\n", -1))
      line(l);
  }

  Path write() {
    try {
      Files.createDirectories(DIRECTORY);
      final Path file = DIRECTORY.resolve("results.md");
      Files.writeString(file, out.toString(), StandardCharsets.UTF_8);
      return file;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void line(final String line) {
    out.append(line).append('\n');
    LogManager.instance().log(this, Level.INFO, "%s", line);
  }

  private static String cell(final String value) {
    return value == null ? "" : value.replace("|", "\\|").replace("\n", " ");
  }
}
