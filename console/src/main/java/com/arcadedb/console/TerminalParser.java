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
package com.arcadedb.console;

import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * Splits the text typed on the console (or read from a script) into the single commands to execute. The separator is the
 * semicolon, but a semicolon found inside a string, a line comment or a block comment is part of the text, not a separator.
 * Comments are dropped while splitting, so they never reach the query engine (issue #5457).
 * <p>
 * The line comment marker depends on the language in use: SQL uses `--`, while Cypher, Gremlin and Mongo use `//`. This matters
 * because a double dash is a legal undirected relationship in Cypher (`(a) -- (b)`) and a double slash is a division in SQL.
 */
public class TerminalParser extends DefaultParser {
  private static final String SQL_LINE_COMMENT   = "--";
  private static final String OTHER_LINE_COMMENT = "//";

  private String  lineComment           = SQL_LINE_COMMENT;
  private boolean lineCommentNeedsBlank = true;
  private boolean blockCommentOpen      = false;

  /**
   * Returns true if the text of the last parse ends inside a block comment, so the following lines are still part of it. Valid
   * only right after a call to {@link #parse(String, int, ParseContext)}, it is used to load scripts line by line (issue #5457).
   */
  public boolean isBlockCommentOpen() {
    return blockCommentOpen;
  }

  /**
   * Sets the language in use, to pick the right line comment marker. Called by the console on `set language = <name>`.
   */
  public void setLanguage(final String language) {
    final boolean sql = language == null || language.toLowerCase(Locale.ENGLISH).startsWith("sql");
    lineComment = sql ? SQL_LINE_COMMENT : OTHER_LINE_COMMENT;
    // THE SQL GRAMMAR READS A LINE COMMENT AS `--` FOLLOWED BY A SPACE, SO `1--2` STAYS ARITHMETIC
    lineCommentNeedsBlank = sql;
  }

  @Override
  public boolean isDelimiterChar(final CharSequence buffer, final int pos) {
    return buffer.charAt(pos) == ';';
  }

  /**
   * Returns true if a line comment starts at the given position. With SQL the marker must be followed by a blank, exactly like in
   * the engine grammar, so that two dashes glued to an operand (`1--2`) remain arithmetic. The end of the text is accepted as a
   * terminator too: dropping such a comment is always safer than forwarding it to the parser.
   */
  private boolean isLineCommentStart(final String line, final int pos) {
    if (line.charAt(pos) != lineComment.charAt(0) || pos + 1 >= line.length() || line.charAt(pos + 1) != lineComment.charAt(1))
      return false;
    return !lineCommentNeedsBlank || pos + 2 >= line.length() || Character.isWhitespace(line.charAt(pos + 2));
  }

  private static boolean isBlockCommentStart(final String line, final int pos) {
    return line.charAt(pos) == '/' && pos + 1 < line.length() && line.charAt(pos + 1) == '*';
  }

  @Override
  public ParsedLine parse(final String line, final int cursor, final ParseContext context) {
    if (line == null)
      return null;

    final List<String> words = new LinkedList();
    final StringBuilder current = new StringBuilder();
    int wordCursor = -1;
    int wordIndex = -1;
    int quoteStart = -1;
    int rawWordCursor = -1;
    int rawWordLength = -1;
    int rawWordStart = 0;
    int braceDepth = 0;
    boolean insideLineComment = false;
    boolean insideBlockComment = false;

    for (int i = 0; i < line.length(); ++i) {
      if (i == cursor) {
        wordIndex = words.size();
        wordCursor = current.length();
        rawWordCursor = i - rawWordStart;
      }

      final char c = line.charAt(i);

      if (insideLineComment) {
        // KEEP THE LINE TERMINATOR SO THE FOLLOWING TEXT IS NOT GLUED TO THE COMMANDED LINE
        if (c == '\n' || c == '\r') {
          insideLineComment = false;
          current.append(c);
        }
      } else if (insideBlockComment) {
        if (c == '*' && i + 1 < line.length() && line.charAt(i + 1) == '/') {
          insideBlockComment = false;
          ++i;
        }
      } else if (quoteStart < 0 && isLineCommentStart(line, i)) {
        insideLineComment = true;
        ++i;
      } else if (quoteStart < 0 && isBlockCommentStart(line, i)) {
        insideBlockComment = true;
        ++i;
      } else if (quoteStart < 0 && this.isQuoteChar(line, i)) {
        quoteStart = i;
        current.append(c);
      } else if (quoteStart >= 0) {
        if (line.charAt(quoteStart) == c && !this.isEscaped(line, i)) {
          current.append(c);
          quoteStart = -1;
          if (rawWordCursor >= 0 && rawWordLength < 0) {
            rawWordLength = i - rawWordStart + 1;
          }
        } else if (!this.isEscapeChar(line, i)) {
          current.append(c);
        }
      } else if (this.isDelimiter(line, i) && braceDepth == 0) {
        if (current.length() > 0) {
          words.add(current.toString());
          current.setLength(0);
          if (rawWordCursor >= 0 && rawWordLength < 0) {
            rawWordLength = i - rawWordStart;
          }
        }

        rawWordStart = i + 1;
      } else if (!this.isEscapeChar(line, i)) {
        if (c == '{') {
          braceDepth++;
          current.append(c);
        } else if (c == '}') {
          final int prevDepth = braceDepth;
          braceDepth--;
          current.append(c);

          // Check if we just closed all braces and there's more content after newlines
          if (prevDepth == 1 && braceDepth == 0 && current.length() > 0) {
            // Look ahead to see if there's a newline followed by non-whitespace content
            int j = i + 1;
            boolean foundNewline = false;
            boolean foundContent = false;

            while (j < line.length() && Character.isWhitespace(line.charAt(j))) {
              if (line.charAt(j) == '\n' || line.charAt(j) == '\r') {
                foundNewline = true;
              }
              j++;
            }

            if (j < line.length() && !this.isDelimiter(line, j)) {
              foundContent = true;
            }

            // If we found a newline and then more content (not a semicolon), split here
            if (foundNewline && foundContent) {
              words.add(current.toString());
              current.setLength(0);
              if (rawWordCursor >= 0 && rawWordLength < 0) {
                rawWordLength = i - rawWordStart + 1;
              }
              rawWordStart = j;
              i = j - 1; // Will be incremented by the loop
            }
          }
        } else {
          current.append(c);
        }
      }
    }

    blockCommentOpen = insideBlockComment;

    if (current.length() > 0 || cursor == line.length()) {
      words.add(current.toString());
      if (rawWordCursor >= 0 && rawWordLength < 0) {
        rawWordLength = line.length() - rawWordStart;
      }
    }

    if (cursor == line.length()) {
      wordIndex = words.size() - 1;
      wordCursor = words.getLast().length();
      rawWordCursor = cursor - rawWordStart;
      rawWordLength = rawWordCursor;
    }

    final String openingQuote = quoteStart >= 0 ? line.substring(quoteStart, quoteStart + 1) : null;
    return new DefaultParser.ArgumentList(line, words, wordIndex, wordCursor, cursor, openingQuote, rawWordCursor, rawWordLength);
  }
}
