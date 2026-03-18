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
package com.arcadedb.server.http.handler.batch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses CSV input into BatchRecord instances with minimal allocation.
 * <p>
 * Expected format:
 * <pre>
 * @type,@class,@id,name,age
 * vertex,Person,t1,Alice,30
 * vertex,Person,t2,Bob,25
 * ---
 * @type,@class,@from,@to,since
 * edge,KNOWS,t1,t2,2020
 * </pre>
 * The first line is the header. A {@code ---} sentinel row separates vertex and edge sections.
 * A new header row is expected after the sentinel.
 * <p>
 * Handles RFC 4180 quoting (double-quote escaping) for single-line fields.
 */
public class CsvBatchRecordStream implements BatchRecordStream {

  private final BufferedReader reader;
  private final BatchRecord    record;
  private       String[]       headers;
  private       int            typeCol   = -1;
  private       int            classCol  = -1;
  private       int            idCol     = -1;
  private       int            fromCol   = -1;
  private       int            toCol     = -1;
  private       int            lineNumber;
  private       boolean        ready;
  private       boolean        headerParsed;

  public CsvBatchRecordStream(final InputStream input) {
    this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8), 65536);
    this.record = new BatchRecord();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (ready)
      return true;

    while (true) {
      final String line = reader.readLine();
      if (line == null)
        return false;

      lineNumber++;

      if (line.isBlank())
        continue;

      // Check for section separator
      if (line.startsWith("---")) {
        headerParsed = false;
        continue;
      }

      // Parse header if needed
      if (!headerParsed) {
        parseHeader(line);
        continue;
      }

      parseLine(line);
      ready = true;
      return true;
    }
  }

  @Override
  public BatchRecord next() {
    ready = false;
    return record;
  }

  @Override
  public int getLineNumber() {
    return lineNumber;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  private void parseHeader(final String line) {
    final List<String> cols = parseCsvFields(line);
    headers = cols.toArray(new String[0]);

    typeCol = -1;
    classCol = -1;
    idCol = -1;
    fromCol = -1;
    toCol = -1;

    for (int i = 0; i < headers.length; i++) {
      switch (headers[i]) {
      case "@type" -> typeCol = i;
      case "@class" -> classCol = i;
      case "@id" -> idCol = i;
      case "@from" -> fromCol = i;
      case "@to" -> toCol = i;
      }
    }

    if (typeCol < 0)
      throw new IllegalArgumentException("CSV header missing @type column at line " + lineNumber);
    if (classCol < 0)
      throw new IllegalArgumentException("CSV header missing @class column at line " + lineNumber);

    headerParsed = true;
  }

  private void parseLine(final String line) {
    record.reset();

    final List<String> fields = parseCsvFields(line);

    if (fields.size() < headers.length)
      throw new IllegalArgumentException("Too few fields at line " + lineNumber + ": expected " + headers.length + ", got " + fields.size());

    final String type = fields.get(typeCol);
    if ("vertex".equals(type) || "v".equals(type)) {
      record.kind = BatchRecord.Kind.VERTEX;
    } else if ("edge".equals(type) || "e".equals(type)) {
      record.kind = BatchRecord.Kind.EDGE;
    } else
      throw new IllegalArgumentException("Unknown @type '" + type + "' at line " + lineNumber);

    record.typeName = fields.get(classCol);
    if (record.typeName == null || record.typeName.isEmpty())
      throw new IllegalArgumentException("Missing @class at line " + lineNumber);

    if (record.kind == BatchRecord.Kind.VERTEX) {
      if (idCol >= 0) {
        final String id = fields.get(idCol);
        record.tempId = id.isEmpty() ? null : id;
      }
    } else {
      if (fromCol < 0 || toCol < 0)
        throw new IllegalArgumentException("Edge section header missing @from or @to column at line " + lineNumber);
      record.fromRef = fields.get(fromCol);
      record.toRef = fields.get(toCol);
      if (record.fromRef.isEmpty() || record.toRef.isEmpty())
        throw new IllegalArgumentException("Edge missing @from or @to value at line " + lineNumber);
    }

    // Extract properties (skip meta columns)
    for (int i = 0; i < headers.length && i < fields.size(); i++) {
      if (i == typeCol || i == classCol || i == idCol || i == fromCol || i == toCol)
        continue;
      final String value = fields.get(i);
      if (!value.isEmpty())
        record.addProperty(headers[i], parseValue(value));
    }
  }

  /**
   * Attempts to parse CSV field values as typed values (number, boolean, null).
   * Falls back to String.
   */
  static Object parseValue(final String value) {
    if (value.isEmpty())
      return null;

    // Try boolean
    if ("true".equalsIgnoreCase(value))
      return Boolean.TRUE;
    if ("false".equalsIgnoreCase(value))
      return Boolean.FALSE;

    // Try integer (long)
    if (value.length() <= 18 && isNumericStart(value)) {
      try {
        return Long.parseLong(value);
      } catch (final NumberFormatException ignored) {
        // fall through
      }
    }

    // Try floating point
    if (isFloatCandidate(value)) {
      try {
        return Double.parseDouble(value);
      } catch (final NumberFormatException ignored) {
        // fall through
      }
    }

    return value;
  }

  private static boolean isNumericStart(final String s) {
    final char c = s.charAt(0);
    return (c >= '0' && c <= '9') || c == '-' || c == '+';
  }

  private static boolean isFloatCandidate(final String s) {
    if (!isNumericStart(s))
      return false;
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c == '.' || c == 'e' || c == 'E')
        return true;
    }
    return false;
  }

  /**
   * Lightweight RFC 4180 CSV field parser. Handles quoted fields with escaped double-quotes.
   * Single-line only (no multi-line quoted fields).
   */
  static List<String> parseCsvFields(final String line) {
    final List<String> fields = new ArrayList<>();
    final int len = line.length();
    int pos = 0;

    while (pos <= len) {
      if (pos == len) {
        // Trailing comma produces empty field
        fields.add("");
        break;
      }

      if (line.charAt(pos) == '"') {
        // Quoted field
        final StringBuilder sb = new StringBuilder();
        pos++; // skip opening quote
        while (pos < len) {
          final char c = line.charAt(pos);
          if (c == '"') {
            if (pos + 1 < len && line.charAt(pos + 1) == '"') {
              sb.append('"');
              pos += 2;
            } else {
              pos++; // skip closing quote
              break;
            }
          } else {
            sb.append(c);
            pos++;
          }
        }
        fields.add(sb.toString());
        // Skip comma after quoted field
        if (pos < len && line.charAt(pos) == ',')
          pos++;
        else
          break;
      } else {
        // Unquoted field
        final int commaIdx = line.indexOf(',', pos);
        if (commaIdx < 0) {
          fields.add(line.substring(pos));
          break;
        }
        fields.add(line.substring(pos, commaIdx));
        pos = commaIdx + 1;
      }
    }

    return fields;
  }
}
