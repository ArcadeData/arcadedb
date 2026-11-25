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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.exception.CommandSQLParsingException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.Locale;

/**
 * ANTLR error listener that converts ANTLR parse errors to CommandSQLParsingException.
 * Provides clear, user-friendly error messages for SQL syntax errors.
 */
public class SQLErrorListener extends BaseErrorListener {

  /**
   * The most characters of the offending line the message quotes, so the message fits a transport with a size limit, such
   * as the gRPC metadata (issue #12567).
   */
  static final int QUOTE_BUDGET = 4 * 1024;

  /**
   * The characters before the failing character a cut quote keeps, so the caret points into the quoted text.
   */
  static final int QUOTE_WINDOW = 512;

  private final String sqlText;

  public SQLErrorListener(final String sqlText) {
    this.sqlText = sqlText;
  }

  @Override
  public void syntaxError(
      final Recognizer<?, ?> recognizer,
      final Object offendingSymbol,
      final int line,
      final int charPositionInLine,
      final String msg,
      final RecognitionException e
  ) {
    // Check for specific error patterns and provide user-friendly messages
    String customMessage = detectCommonErrors(msg, charPositionInLine);

    // Build a user-friendly error message
    final StringBuilder errorMessage = new StringBuilder();
    errorMessage.append("SQL syntax error at line ").append(line);
    errorMessage.append(", column ").append(charPositionInLine);
    errorMessage.append(": ");

    if (customMessage != null) {
      errorMessage.append(customMessage);
    } else {
      errorMessage.append(msg);
    }

    // Add a snippet of the SQL text showing where the error occurred
    if (sqlText != null && !sqlText.isEmpty()) {
      final String[] lines = sqlText.split("\n");
      if (line > 0 && line <= lines.length) {
        final String errorLine = lines[line - 1];
        final Snippet snippet = snippetOf(errorLine, charPositionInLine);
        errorMessage.append("\n");
        errorMessage.append(snippet.text);
        errorMessage.append("\n");

        // The caret points at the failing character, under the quoted text
        for (int i = 0; i < snippet.caretOffset; i++) {
          errorMessage.append(" ");
        }
        errorMessage.append("^");
      }
    }

    throw new CommandSQLParsingException(errorMessage.toString());
  }

  /**
   * The offending line, quoted whole when it fits {@link #QUOTE_BUDGET}, otherwise cut around the failing character with
   * the caret placed under the cut quote.
   */
  private static Snippet snippetOf(final String errorLine, final int charPositionInLine) {
    if (errorLine.length() <= QUOTE_BUDGET)
      return new Snippet(errorLine, charPositionInLine);

    final int position = Math.max(0, Math.min(charPositionInLine, errorLine.length() - 1));
    int start = Math.max(0, position - QUOTE_WINDOW);
    // A cut never splits a surrogate pair
    if (start > 0 && Character.isLowSurrogate(errorLine.charAt(start)))
      --start;
    int end = Math.min(errorLine.length(), start + QUOTE_BUDGET);
    if (end < errorLine.length() && Character.isLowSurrogate(errorLine.charAt(end)))
      --end;
    final StringBuilder out = new StringBuilder(QUOTE_BUDGET + 32);
    if (start > 0)
      out.append("...");
    out.append(errorLine, start, end);
    if (end < errorLine.length())
      out.append("...");
    return new Snippet(out.toString(), position - start + (start > 0 ? 3 : 0));
  }

  /**
   * A quoted piece of the offending line and where the caret goes under it.
   */
  private static final class Snippet {
    private final String text;
    private final int caretOffset;

    private Snippet(final String text, final int caretOffset) {
      this.text = text;
      this.caretOffset = caretOffset;
    }
  }

  /**
   * Detects common SQL errors and provides user-friendly messages.
   * Returns null if no specific pattern is detected.
   */
  private String detectCommonErrors(final String msg, final int charPositionInLine) {
    if (msg == null || sqlText == null) {
      return null;
    }

    final String msgLower = msg.toLowerCase(Locale.ROOT);

    // Detect "AND and" or "OR and" patterns (duplicate operators)
    // ANTLR may report this as "extraneous input", "no viable alternative", or similar
    if (msgLower.contains("'and'") || msgLower.contains("'or'")) {
      // Look at the context around the error position to determine which operator is duplicated
      if (charPositionInLine > 0 && charPositionInLine <= sqlText.length()) {
        final String before = sqlText.substring(Math.max(0, charPositionInLine - 10), charPositionInLine).toUpperCase(Locale.ROOT).trim();
        if (before.endsWith("AND") || before.contains("AND ")) {
          return "AND operator must be followed by a condition";
        } else if (before.endsWith("OR") || before.contains("OR ")) {
          return "OR operator must be followed by a condition";
        }
      }
      // Default to AND if we can't determine from context
      if (msgLower.contains("'and'")) {
        return "AND operator must be followed by a condition";
      } else {
        return "OR operator must be followed by a condition";
      }
    }

    // Detect missing condition after AND/OR at end of query
    if (msgLower.contains("missing") || msgLower.contains("expecting") || msgLower.contains("no viable alternative")) {
      if (charPositionInLine > 0 && charPositionInLine <= sqlText.length()) {
        final String before = sqlText.substring(Math.max(0, charPositionInLine - 10), Math.min(sqlText.length(), charPositionInLine)).toUpperCase(Locale.ROOT).trim();
        if (before.endsWith("AND")) {
          return "AND operator must be followed by a condition";
        } else if (before.endsWith("OR")) {
          return "OR operator must be followed by a condition";
        }
      }
    }

    return null;
  }
}
