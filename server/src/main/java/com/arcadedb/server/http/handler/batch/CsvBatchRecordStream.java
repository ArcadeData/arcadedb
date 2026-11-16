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

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

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
 * Every other column is a property. The {@code @} prefix is what separates control from data, so a column that
 * begins with {@code @} and is not one of the five understood names is refused at the header rather than loaded as a
 * property with that name (issue #7570).
 * <p>
 * Handles RFC 4180 quoting (double-quote escaping) for single-line fields.
 */
public class CsvBatchRecordStream implements BatchRecordStream {
  private static final int NOT_A_NUMBER  = 0;
  private static final int JSON_INTEGER  = 1;
  private static final int JSON_FRACTION = 2;

  private final BufferedReader reader;
  private final BatchRecord    record;
  private final Schema         schema;
  private       String[]       headers;
  private       String         columnTypesFor;
  private       Type[]         columnTypes;
  private       int            typeCol   = -1;
  private       int            classCol  = -1;
  private       int            idCol     = -1;
  private       int            fromCol   = -1;
  private       int            toCol     = -1;
  private       int            lineNumber;
  private       long           linesSkipped;
  private       boolean        ready;
  private       boolean        headerParsed;

  /** A stream with no schema to read against: every property column is read as undeclared. */
  public CsvBatchRecordStream(final InputStream input) {
    this(input, null);
  }

  /**
   * @param schema the schema of the database the load writes into, which decides how each field is read: a CSV field is
   *               text, and the declared type of its property is what says whether that text is a number (issue #11802).
   *               {@code null} reads every column as undeclared
   */
  public CsvBatchRecordStream(final InputStream input, final Schema schema) {
    this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8), 65536);
    this.record = new BatchRecord();
    this.schema = schema;
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

      if (line.isBlank()) {
        linesSkipped++;
        continue;
      }

      // Check for section separator
      if (line.startsWith("---")) {
        headerParsed = false;
        linesSkipped++;
        continue;
      }

      // Parse header if needed
      if (!headerParsed) {
        parseHeader(line);
        linesSkipped++;
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
  public long getLinesRead() {
    return lineNumber;
  }

  @Override
  public long getLinesSkipped() {
    return linesSkipped;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  private void parseHeader(final String line) {
    final List<String> cols = parseCsvFields(line);
    headers = cols.toArray(new String[0]);
    // The cached column types are positional: a new header invalidates them
    columnTypesFor = null;
    columnTypes = null;

    typeCol = -1;
    classCol = -1;
    idCol = -1;
    fromCol = -1;
    toCol = -1;

    for (int i = 0; i < headers.length; i++) {
      switch (headers[i]) {
      case BatchControlKeys.TYPE -> typeCol = i;
      case BatchControlKeys.CLASS -> classCol = i;
      case BatchControlKeys.ID -> idCol = i;
      case BatchControlKeys.FROM -> fromCol = i;
      case BatchControlKeys.TO -> toCol = i;
      default -> rejectReservedColumn(headers[i]);
      }
    }

    if (typeCol < 0)
      throw new IllegalArgumentException("CSV header missing @type column at line " + lineNumber);
    if (classCol < 0)
      throw new IllegalArgumentException("CSV header missing @class column at line " + lineNumber);

    headerParsed = true;
  }

  /**
   * Refuses an unrecognised {@code @}-prefixed column. CSV names its control keys in the header, so the header is
   * where the JSONL reserved-key rule has to be applied: an unknown one used to become a property column, and a load
   * built on it answered 200 holding a property whose name starts with {@code @} (issue #7570).
   * <p>
   * There is no CSV counterpart of the nested {@code properties} refusal the JSONL parser also makes: every value
   * {@link #parseValue} produces is a scalar, never a {@link java.util.Map}, so a {@code properties} column can only
   * ever be ordinary data.
   */
  private void rejectReservedColumn(final String column) {
    if (!column.isEmpty() && column.charAt(0) == '@')
      throw new IllegalArgumentException("Unknown control column '" + column + "' in the CSV header at line "
          + lineNumber + ": the '@' prefix is reserved by the batch encoding and only "
          + BatchControlKeys.LIST + " are understood. A property column cannot start with '@': rename the "
          + "column, or drop it");
  }

  private void parseLine(final String line) {
    record.reset();

    final List<String> fields = parseCsvFields(line);

    if (fields.size() < headers.length)
      throw new MalformedBatchRecordException("Too few fields at line " + lineNumber + ": expected " + headers.length + ", got " + fields.size());

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
      rejectMisplacedControlValue(fields, fromCol, BatchControlKeys.FROM, BatchRecord.Kind.VERTEX);
      rejectMisplacedControlValue(fields, toCol, BatchControlKeys.TO, BatchRecord.Kind.VERTEX);
      if (idCol >= 0) {
        final String id = fields.get(idCol);
        record.tempId = id.isEmpty() ? null : id;
      }
    } else {
      rejectMisplacedControlValue(fields, idCol, BatchControlKeys.ID, BatchRecord.Kind.EDGE);
      if (fromCol < 0 || toCol < 0)
        throw new IllegalArgumentException("Edge section header missing @from or @to column at line " + lineNumber);
      record.fromRef = fields.get(fromCol);
      record.toRef = fields.get(toCol);
      if (record.fromRef.isEmpty() || record.toRef.isEmpty())
        throw new IllegalArgumentException("Edge missing @from or @to value at line " + lineNumber);
    }

    // Extract properties (skip meta columns), each read for the type its property declares (issue #11802)
    final Type[] declared = columnTypesOf(record.typeName);
    for (int i = 0; i < headers.length && i < fields.size(); i++) {
      if (i == typeCol || i == classCol || i == idCol || i == fromCol || i == toCol)
        continue;
      final String value = fields.get(i);
      if (!value.isEmpty())
        record.addProperty(headers[i], parseValue(value, declared[i]));
    }
  }

  /**
   * Refuses a NON-EMPTY value in a control column the row's kind cannot use (issue #7574).
   * <p>
   * Keyed on the value rather than on the column, unlike the JSONL rule's key test, and that is deliberate: the
   * documented CSV shape puts a fresh header after the {@code ---} sentinel, but a single header carrying all five
   * control columns across both sections works today and is a reasonable thing for a client to emit, with empty
   * {@code @from}/{@code @to} on the vertex rows. Refusing the column would break it; refusing only a value that
   * was actually sent leaves it working and still catches the drop.
   */
  private void rejectMisplacedControlValue(final List<String> fields, final int column, final String key,
      final BatchRecord.Kind kind) {
    if (column >= 0 && column < fields.size() && !fields.get(column).isEmpty())
      throw BatchControlKeys.misplaced(key, kind, lineNumber);
  }

  /**
   * The declared type of every property column for the row's type, or {@code null} for a column the schema does not
   * declare. Cached for the last type seen, since a CSV section is typically one type after another: a row of the same
   * type as the previous one pays no schema lookup at all.
   */
  private Type[] columnTypesOf(final String typeName) {
    if (typeName.equals(columnTypesFor))
      return columnTypes;

    final DocumentType type = schema != null ? schema.getTypeOrNull(typeName) : null;
    final Type[] types = new Type[headers.length];
    if (type != null)
      for (int i = 0; i < headers.length; i++) {
        final Property property = type.getPolymorphicPropertyIfExists(headers[i]);
        if (property != null)
          types[i] = property.getType();
      }
    columnTypesFor = typeName;
    columnTypes = types;
    return types;
  }

  /**
   * Reads a CSV field, which is text, for a property of the given declared type (issue #11802).
   * <ul>
   *   <li>{@code STRING} and {@code DECIMAL} take the text exactly as sent: any number read from it first would drop what
   *   the text carries and a number cannot ({@code 007}, {@code 1.50}, {@code False}), and the declared conversion of the
   *   text itself is exact ({@code new BigDecimal("1.50")} keeps its scale)</li>
   *   <li>any other declared type takes the most lenient reading ({@link #parseLenient}), which the declared conversion then
   *   turns into the property's type exactly as before: {@code TRUE} for a {@code BOOLEAN}, {@code 007} or {@code +5} for
   *   an {@code INTEGER}, epoch milliseconds for a {@code DATETIME}</li>
   *   <li>an undeclared property has nothing but the text to go by, so the field is read as the JSON literal it spells, if it
   *   spells one ({@link #parseValue(String)}): the reading a JSONL load of the same value as a JSON literal gets</li>
   * </ul>
   */
  static Object parseValue(final String value, final Type declared) {
    if (value.isEmpty())
      return null;
    if (declared == null)
      return parseValue(value);
    if (declared == Type.STRING || declared == Type.DECIMAL)
      return value;
    return parseLenient(value);
  }

  /**
   * The reading of a field in a column the schema does not declare: {@code true} and {@code false} as booleans, a JSON
   * number as a {@code Long} (an integer that fits one) or a {@code Double}, anything else as the text it is. JSON's own
   * spelling is the line because it is the one a client already knows means "a number" and not "text that looks like one":
   * it has no leading zeros ({@code 007}, {@code 000}), no leading {@code +}, no bare trailing dot ({@code 5.}), and its
   * booleans are lowercase, so none of those is rewritten into a value that prints back differently (issue #11802). An
   * integer beyond the {@code long} range stays text rather than losing digits to a double.
   */
  static Object parseValue(final String value) {
    if (value.isEmpty())
      return null;

    if ("true".equals(value))
      return Boolean.TRUE;
    if ("false".equals(value))
      return Boolean.FALSE;

    final int shape = jsonNumberShape(value);
    if (shape == NOT_A_NUMBER)
      return value;
    try {
      return shape == JSON_INTEGER ? (Object) Long.parseLong(value) : (Object) Double.parseDouble(value);
    } catch (final NumberFormatException e) {
      // An integer outside the long range: kept whole as text
      return value;
    }
  }

  /**
   * Classifies {@code s} by the JSON number grammar ({@code -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?}) in one pass and
   * with no allocation.
   */
  private static int jsonNumberShape(final String s) {
    final int len = s.length();
    int i = 0;
    if (s.charAt(i) == '-' && ++i == len)
      return NOT_A_NUMBER;

    final char first = s.charAt(i);
    if (first == '0')
      i++;
    else if (first >= '1' && first <= '9')
      while (i < len && isDigit(s.charAt(i)))
        i++;
    else
      return NOT_A_NUMBER;

    if (i == len)
      return JSON_INTEGER;

    if (s.charAt(i) == '.') {
      final int fractionStart = ++i;
      while (i < len && isDigit(s.charAt(i)))
        i++;
      if (i == fractionStart)
        return NOT_A_NUMBER;
    }

    if (i < len && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
      if (++i < len && (s.charAt(i) == '+' || s.charAt(i) == '-'))
        i++;
      final int exponentStart = i;
      while (i < len && isDigit(s.charAt(i)))
        i++;
      if (i == exponentStart)
        return NOT_A_NUMBER;
    }

    return i == len ? JSON_FRACTION : NOT_A_NUMBER;
  }

  private static boolean isDigit(final char c) {
    return c >= '0' && c <= '9';
  }

  /**
   * The reading for a declared type other than {@code STRING}/{@code DECIMAL}: booleans in any case, and anything
   * {@link Long#parseLong} or {@link Double#parseDouble} accepts as a number. The declared conversion that follows is what
   * gives the value its type, so being generous here only widens what the type accepts.
   */
  private static Object parseLenient(final String value) {
    if ("true".equalsIgnoreCase(value))
      return Boolean.TRUE;
    if ("false".equalsIgnoreCase(value))
      return Boolean.FALSE;

    if (isNumericStart(value)) {
      try {
        return isFloatCandidate(value) ? (Object) Double.parseDouble(value) : (Object) Long.parseLong(value);
      } catch (final NumberFormatException ignored) {
        // Not a number after all: the declared conversion is handed the text
      }
    }

    return value;
  }

  private static boolean isNumericStart(final String s) {
    final char c = s.charAt(0);
    return isDigit(c) || c == '-' || c == '+';
  }

  private static boolean isFloatCandidate(final String s) {
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
