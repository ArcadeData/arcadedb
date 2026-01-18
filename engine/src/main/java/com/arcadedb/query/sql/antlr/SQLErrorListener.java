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

/**
 * ANTLR error listener that converts ANTLR parse errors to CommandSQLParsingException.
 * Provides clear, user-friendly error messages for SQL syntax errors.
 */
public class SQLErrorListener extends BaseErrorListener {

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
    // Build a user-friendly error message
    final StringBuilder errorMessage = new StringBuilder();
    errorMessage.append("SQL syntax error at line ").append(line);
    errorMessage.append(", column ").append(charPositionInLine);
    errorMessage.append(": ").append(msg);

    // Add a snippet of the SQL text showing where the error occurred
    if (sqlText != null && !sqlText.isEmpty()) {
      final String[] lines = sqlText.split("\n");
      if (line > 0 && line <= lines.length) {
        final String errorLine = lines[line - 1];
        errorMessage.append("\n");
        errorMessage.append(errorLine);
        errorMessage.append("\n");

        // Add a caret (^) pointing to the error position
        for (int i = 0; i < charPositionInLine; i++) {
          errorMessage.append(" ");
        }
        errorMessage.append("^");
      }
    }

    throw new CommandSQLParsingException(errorMessage.toString());
  }
}
