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
package com.arcadedb.integration.importer.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link GraphImporter.RecordSource} that reads CSV files with a header row.
 * Uses the first line as column names; subsequent lines are data records.
 * Supports configurable delimiter (default: comma). The delimiter is matched as a literal character, so any
 * character is a usable field separator - {@code '|'} and {@code '.'} included (issue #7267).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CsvRowSource implements GraphImporter.RecordSource {

  private final String filePath;
  private final char   delimiter;
  private final int    skipLines;

  public CsvRowSource(final String filePath) {
    this(filePath, ',', 0);
  }

  public CsvRowSource(final String filePath, final char delimiter, final int skipLines) {
    this.filePath = filePath;
    this.delimiter = delimiter;
    this.skipLines = skipLines;
  }

  public static CsvRowSource from(final String dir, final String fileName) {
    return new CsvRowSource(new File(dir, fileName).getPath());
  }

  public static CsvRowSource from(final String dir, final String fileName, final char delimiter) {
    return new CsvRowSource(new File(dir, fileName).getPath(), delimiter, 0);
  }

  @Override
  public Character fieldSeparator() {
    return delimiter;
  }

  @Override
  public void forEach(final GraphImporter.RecordVisitor visitor) throws Exception {
    try (final BufferedReader br = new BufferedReader(
        new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8), 1 << 16)) {

      // Skip leading lines
      for (int i = 0; i < skipLines; i++)
        br.readLine();

      // Read header
      final String headerLine = br.readLine();
      if (headerLine == null)
        return;
      final String[] headers = splitLine(headerLine);

      final CsvRecordReader rec = new CsvRecordReader();
      String line;
      while ((line = br.readLine()) != null) {
        if (line.isEmpty())
          continue;
        final String[] values = splitLine(line);
        rec.fields.clear();
        for (int i = 0; i < headers.length && i < values.length; i++)
          rec.fields.put(headers[i], values[i]);
        visitor.visit(rec);
      }
    }
  }

  /**
   * Splits a line on the delimiter as a LITERAL character, keeping every empty field - the shape
   * {@code String.split(literal, -1)} answers, which the header/value zip in {@link #forEach} is written against.
   * <p>
   * Issue #7267: do not go back to {@code line.split(String.valueOf(delimiter), -1)}. Its first argument is a
   * <b>regular expression</b>, and the delimiter is a character the operator chooses, so a metacharacter was read
   * as itself only by accident - {@code '|'} made every character its own field, {@code '.'} annihilated the row,
   * {@code '$'} and {@code '^'} split nothing at all, and eight more threw {@code PatternSyntaxException} about a
   * pattern nobody wrote. The first four were the worse half, because a shredded header means every
   * {@code get(attribute)} misses and the import yields property-less vertices in silence. The full table is in
   * {@code docs/7267-csvrowsource-regex-delimiter.md}.
   * <p>
   * {@code indexOf} is also cheaper than what it replaces: {@code String.split} takes its regex-free fast path only
   * for a single <i>non-metacharacter</i> char and compiles a {@code Pattern} per call - per row of a bulk import -
   * for the rest. {@code Pattern.quote} was rejected for the mirror image of that reason: {@code \Q;\E} is not a
   * single character, so it would compile a {@code Pattern} for every delimiter, the default comma included.
   * <p>
   * Quoting is still unsupported, exactly as before - a delimiter inside a field value still separates fields. For
   * quoted CSV use the Univocity-backed {@code CSVImporterFormat}.
   */
  private String[] splitLine(final String line) {
    int fields = 1;
    for (int pos = line.indexOf(delimiter); pos >= 0; pos = line.indexOf(delimiter, pos + 1))
      ++fields;

    final String[] values = new String[fields];
    int start = 0;
    for (int i = 0; i < fields - 1; ++i) {
      final int pos = line.indexOf(delimiter, start);
      values[i] = line.substring(start, pos);
      start = pos + 1;
    }
    values[fields - 1] = line.substring(start);

    return values;
  }

  private static class CsvRecordReader implements GraphImporter.RecordReader {
    final Map<String, String> fields = new HashMap<>();

    /** Empty means not set, as it does on every other source: see {@link GraphImporter.RecordReader#get} (#7332). */
    @Override
    public String get(final String attribute) {
      return GraphImporter.RecordReader.emptyAsNull(fields.get(attribute));
    }

    @Override
    public int getInt(final String attribute) {
      final String v = fields.get(attribute);
      return v != null && !v.isEmpty() ? Integer.parseInt(v) : 0;
    }
  }
}
