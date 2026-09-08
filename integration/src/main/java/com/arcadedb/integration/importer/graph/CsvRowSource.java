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
   * Issue #7267: this used to be {@code line.split(String.valueOf(delimiter), -1)}, whose first argument is a
   * <b>regular expression</b>. The delimiter is a character the operator chooses, so every one that happens to be a
   * regex metacharacter did something other than separate fields, and never said which character was to blame:
   * {@code '|'} is an alternation of two empty branches, so every character became its own field, separators
   * included; {@code '.'} matched everything and annihilated the row; {@code '$'} and {@code '^'} are anchors and
   * split nothing at all; {@code '*'}, {@code '+'}, {@code '?'}, {@code '('}, {@code ')'}, {@code '['}, {@code '{'}
   * and {@code '\'} threw {@code PatternSyntaxException} about a pattern nobody wrote. The first four are the worse
   * half: a header line cut into single characters means every {@code get(attribute)} misses, so the import produces
   * vertices with no properties, or none, in silence.
   * <p>
   * Walking with {@code indexOf(char, from)} is literal by construction and also cheaper than what it replaces:
   * {@code String.split} takes its regex-free fast path only for a single <i>non-metacharacter</i> char, and
   * compiles a {@code Pattern} per call - that is, per row of a bulk import - for the others. One counting pass
   * sizes the result array exactly, so a row costs one array and its substrings and no {@code Pattern} ever.
   * {@code Pattern.quote} would have been the one-line fix and was rejected for the mirror-image reason: {@code \Q;\E}
   * is not a single character, so it would compile a {@code Pattern} on every row for every delimiter, the default
   * comma included.
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

    @Override
    public String get(final String attribute) {
      final String v = fields.get(attribute);
      return v != null && !v.isEmpty() ? v : null;
    }

    @Override
    public int getInt(final String attribute) {
      final String v = fields.get(attribute);
      return v != null && !v.isEmpty() ? Integer.parseInt(v) : 0;
    }
  }
}
