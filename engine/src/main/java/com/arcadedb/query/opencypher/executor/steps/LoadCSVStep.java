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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LoadCSVClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Execution step for LOAD CSV clause.
 * Reads CSV data from a file or URL and binds each row to a variable.
 * <p>
 * Without WITH HEADERS: each row is a List&lt;String&gt; (access via row[0], row[1], etc.)
 * With WITH HEADERS: each row is a Map&lt;String, Object&gt; (access via row.name or row['name'])
 */
public class LoadCSVStep extends AbstractExecutionStep {
  private final LoadCSVClause loadCSVClause;
  private final ExpressionEvaluator evaluator;

  public LoadCSVStep(final LoadCSVClause loadCSVClause, final CommandContext context,
      final CypherFunctionFactory functionFactory) {
    super(context);
    this.loadCSVClause = loadCSVClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasPrevious = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private BufferedReader currentReader = null;
      private String[] headers = null;
      private Result currentInputRow = null;
      private String currentUrl = null;
      private int currentLineNumber = 0;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (prevResults == null) {
          if (hasPrevious) {
            prevResults = prev.syncPull(context, nRecords);
          } else {
            prevResults = new ResultSet() {
              private boolean consumed = false;

              @Override
              public boolean hasNext() {
                return !consumed;
              }

              @Override
              public Result next() {
                if (consumed)
                  throw new NoSuchElementException();
                consumed = true;
                return new ResultInternal();
              }

              @Override
              public void close() {
              }
            };
          }
        }

        while (buffer.size() < n) {
          // Try to read next CSV line from current reader
          if (currentReader != null) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              final String line = readCSVLine(currentReader);
              if (line != null) {
                currentLineNumber++;
                if (context.isProfiling())
                  rowCount++;
                final List<String> fields = parseCSVLine(line, loadCSVClause.getFieldTerminator());
                final Object rowValue;
                if (loadCSVClause.isWithHeaders()) {
                  final Map<String, Object> map = new LinkedHashMap<>();
                  for (int i = 0; i < headers.length; i++)
                    map.put(headers[i], i < fields.size() ? fields.get(i) : null);
                  rowValue = map;
                } else {
                  rowValue = fields;
                }
                final ResultInternal result = createOutputRow(currentInputRow, rowValue);
                result.setProperty("__loadCSV_file", currentUrl);
                result.setProperty("__loadCSV_linenumber", currentLineNumber);
                buffer.add(result);
                continue;
              }
              // End of file — close reader and move to next input row
              closeReader();
            } catch (final IOException e) {
              closeReader();
              throw new CommandExecutionException("Error reading CSV file: " + currentUrl, e);
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }

          // Need a new input row
          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          currentInputRow = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            final Expression urlExpr = loadCSVClause.getUrlExpression();
            final Object urlValue = evaluator.evaluate(urlExpr, currentInputRow, context);
            if (urlValue == null)
              throw new CommandExecutionException("LOAD CSV URL expression evaluated to null");
            currentUrl = urlValue.toString();
            currentLineNumber = 0;
            currentReader = openReader(currentUrl, context);

            if (loadCSVClause.isWithHeaders()) {
              final String headerLine = readCSVLine(currentReader);
              if (headerLine != null) {
                currentLineNumber++;
                final List<String> headerFields = parseCSVLine(headerLine, loadCSVClause.getFieldTerminator());
                headers = headerFields.toArray(new String[0]);
              } else {
                // Empty file
                closeReader();
              }
            }
          } catch (final IOException e) {
            closeReader();
            throw new CommandExecutionException("Error opening CSV file: " + currentUrl, e);
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        closeReader();
        LoadCSVStep.this.close();
      }

      private void closeReader() {
        if (currentReader != null) {
          try {
            currentReader.close();
          } catch (final IOException ignored) {
          }
          currentReader = null;
        }
      }
    };
  }

  private ResultInternal createOutputRow(final Result inputRow, final Object rowValue) {
    final ResultInternal result = new ResultInternal();
    for (final String prop : inputRow.getPropertyNames())
      result.setProperty(prop, inputRow.getProperty(prop));
    result.setProperty(loadCSVClause.getVariable(), rowValue);
    return result;
  }

  /**
   * Opens a raw InputStream for the given URL string, applying security checks.
   * Supports file:/// URLs, http(s):// URLs, and bare file paths.
   */
  static InputStream openRawInputStream(final String url, final CommandContext context) throws IOException {
    if (url.startsWith("http://") || url.startsWith("https://")) {
      final URL netUrl = URI.create(url).toURL();
      return netUrl.openStream();
    }

    // File-based URL — check security settings
    final boolean allowFileUrls = context.getDatabase().getConfiguration()
        .getValueAsBoolean(GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS);
    if (!allowFileUrls)
      throw new SecurityException("LOAD CSV file:/// URLs are disabled. Set arcadedb.opencypher.loadCsv.allowFileUrls=true to enable.");

    String filePath;
    if (url.startsWith("file:///"))
      filePath = URI.create(url).getPath();
    else
      filePath = url;

    filePath = resolveAndValidatePath(filePath, context);
    return new FileInputStream(filePath);
  }

  /**
   * Resolves a file path against the configured import directory and validates
   * that the resolved path does not escape outside the import directory.
   */
  static String resolveAndValidatePath(final String path, final CommandContext context) throws IOException {
    final String importDir = context.getDatabase().getConfiguration()
        .getValueAsString(GlobalConfiguration.OPENCYPHER_LOAD_CSV_IMPORT_DIRECTORY);

    if (importDir == null || importDir.isEmpty())
      return path; // No restriction

    final Path importDirPath = Path.of(importDir).toAbsolutePath().normalize();
    final Path resolvedPath = importDirPath.resolve(path).normalize().toAbsolutePath();

    if (!resolvedPath.startsWith(importDirPath))
      throw new SecurityException(
          "LOAD CSV path traversal blocked: resolved path '" + resolvedPath + "' is outside import directory '" + importDirPath + "'");

    return resolvedPath.toString();
  }

  /**
   * Opens a BufferedReader for the given URL string, with security checks and compression support.
   * Supports file:/// URLs, http(s):// URLs, and bare file paths.
   * Transparently decompresses .gz and .zip files.
   */
  static BufferedReader openReader(final String url, final CommandContext context) throws IOException {
    InputStream is = openRawInputStream(url, context);

    if (url.endsWith(".gz"))
      is = new GZIPInputStream(is);
    else if (url.endsWith(".zip")) {
      final ZipInputStream zis = new ZipInputStream(is);
      final ZipEntry entry = zis.getNextEntry();
      if (entry == null)
        throw new CommandExecutionException("ZIP file is empty: " + url);
      is = zis;
    }

    return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
  }

  /**
   * Reads a single CSV logical line from the reader, handling quoted fields that span multiple lines.
   * Returns null at end of stream.
   */
  static String readCSVLine(final BufferedReader reader) throws IOException {
    final String firstLine = reader.readLine();
    if (firstLine == null)
      return null;

    // Check if we have an odd number of unescaped quotes (meaning multiline field)
    if (countUnescapedQuotes(firstLine) % 2 == 0)
      return firstLine;

    // Multiline field: keep reading
    final StringBuilder sb = new StringBuilder(firstLine);
    while (true) {
      final String nextLine = reader.readLine();
      if (nextLine == null)
        return sb.toString(); // Unterminated quote — return what we have
      sb.append('\n').append(nextLine);
      if (countUnescapedQuotes(sb.toString()) % 2 == 0)
        return sb.toString();
    }
  }

  /**
   * Counts unescaped quotes in a string, accounting for both "" and \" escape sequences.
   */
  private static int countUnescapedQuotes(final String s) {
    int count = 0;
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == '\\' && i + 1 < s.length() && s.charAt(i + 1) == '"') {
        i++; // Skip backslash-escaped quote
      } else if (s.charAt(i) == '"') {
        count++;
        if (i + 1 < s.length() && s.charAt(i + 1) == '"')
          i++; // Skip double-quote escape
      }
    }
    return count;
  }

  /**
   * Parses a CSV line into fields following RFC 4180 (handles quoted fields with embedded
   * delimiters, newlines, and escaped quotes). Also supports backslash escaping (\").
   */
  static List<String> parseCSVLine(final String line, final String delimiter) {
    final List<String> fields = new ArrayList<>();
    final int len = line.length();
    final int delimLen = delimiter.length();
    int i = 0;

    while (i <= len) {
      if (i == len) {
        // Trailing delimiter — add empty field
        if (fields.isEmpty() || (i > 0 && line.substring(i - delimLen, i).equals(delimiter)))
          fields.add("");
        break;
      }

      if (line.charAt(i) == '"') {
        // Quoted field
        final StringBuilder sb = new StringBuilder();
        i++; // Skip opening quote
        while (i < len) {
          if (line.charAt(i) == '"') {
            if (i + 1 < len && line.charAt(i + 1) == '"') {
              sb.append('"');
              i += 2;
            } else {
              i++; // Skip closing quote
              break;
            }
          } else if (line.charAt(i) == '\\' && i + 1 < len && line.charAt(i + 1) == '"') {
            // Backslash escape: \" → "
            sb.append('"');
            i += 2;
          } else {
            sb.append(line.charAt(i));
            i++;
          }
        }
        fields.add(sb.toString());
        // Skip delimiter after quoted field
        if (i < len && line.startsWith(delimiter, i))
          i += delimLen;
      } else {
        // Unquoted field
        final int delimIndex = line.indexOf(delimiter, i);
        if (delimIndex == -1) {
          fields.add(line.substring(i));
          break;
        } else {
          fields.add(line.substring(i, delimIndex));
          i = delimIndex + delimLen;
        }
      }
    }

    return fields;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ LOAD CSV ");
    if (loadCSVClause.isWithHeaders())
      builder.append("WITH HEADERS ");
    builder.append("FROM ");
    builder.append(loadCSVClause.getUrlExpression().getText());
    builder.append(" AS ");
    builder.append(loadCSVClause.getVariable());

    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }

    return builder.toString();
  }
}
