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
 * Supports configurable delimiter (default: comma).
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

  private String[] splitLine(final String line) {
    // Simple CSV split (no quoting support — for quoted fields use Univocity)
    return line.split(String.valueOf(delimiter), -1);
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
