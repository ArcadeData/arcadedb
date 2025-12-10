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

import java.util.*;

public class TerminalParser extends DefaultParser {

  @Override
  public boolean isDelimiterChar(final CharSequence buffer, final int pos) {
    return buffer.charAt(pos) == ';';
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

    for (int i = 0; i < line.length(); ++i) {
      if (i == cursor) {
        wordIndex = words.size();
        wordCursor = current.length();
        rawWordCursor = i - rawWordStart;
      }

      final char c = line.charAt(i);

      if (quoteStart < 0 && this.isQuoteChar(line, i)) {
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

    if (current.length() > 0 || cursor == line.length()) {
      words.add(current.toString());
      if (rawWordCursor >= 0 && rawWordLength < 0) {
        rawWordLength = line.length() - rawWordStart;
      }
    }

    if (cursor == line.length()) {
      wordIndex = words.size() - 1;
      wordCursor = words.get(words.size() - 1).length();
      rawWordCursor = cursor - rawWordStart;
      rawWordLength = rawWordCursor;
    }

    final String openingQuote = quoteStart >= 0 ? line.substring(quoteStart, quoteStart + 1) : null;
    return new DefaultParser.ArgumentList(line, words, wordIndex, wordCursor, cursor, openingQuote, rawWordCursor, rawWordLength);
  }
}
