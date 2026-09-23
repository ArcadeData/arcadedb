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
 * <p>
 * A semicolon inside a JSON object literal is not a separator either, so the open braces are counted while scanning. The count
 * never goes below zero: an unbalanced closing brace is a typo in one command and must not change how the following ones are
 * split (issue #6392). The opposite typo, a `{` that is never closed, cannot be clamped the same way: the parser genuinely
 * cannot know it is unbalanced until the input ends, so it instead records the offset of that brace for the caller to report
 * once parsing is done (issue #6439).
 * <p>
 * With SQL, a command also ends with no semicolon at the closing brace of a script block (`IF (...) { ... }`, `FOREACH`,
 * `WHILE`) when more content follows on a new line, unless that content is the `ELSE` of the same `IF`. Every other balanced
 * brace pair - a map literal, a `CONTENT { ... }` object, a MATCH pattern - sits inside a statement that can continue on the
 * next line, so it never ends the command: splitting there ran `UPDATE ... SET x = {json}` without the WHERE written on the
 * following line (issue #8246).
 * <p>
 * The backslash is an escape character for the purpose of tracking quotes and delimiters - an escaped quote does not close a
 * string and an escaped semicolon does not split a command - but it is <b>kept</b> in the emitted word rather than consumed.
 * The words this parser produces are handed to the query engine, and the engine, not the console, owns string-literal
 * escaping: consuming one level here made `insert into Doc set p = 'C:\Users\bob'` store `C:Usersbob`, and made any `.sql`
 * script using backslash escaping impossible to replay with `load` (issue #6827). The one place where shell-like unescaping
 * is genuinely wanted is the value of the console's own {@code SET} command, which unescapes it there.
 */
public class TerminalParser extends DefaultParser {
  private static final String SQL_LINE_COMMENT   = "--";
  private static final String OTHER_LINE_COMMENT = "//";

  private String  lineComment           = SQL_LINE_COMMENT;
  private boolean lineCommentNeedsBlank = true;
  private boolean scriptBlocks          = true;
  private boolean blockCommentOpen      = false;
  private int     unbalancedBraceOffset = -1;

  /**
   * Returns true if the text of the last parse ends inside a block comment, so the following lines are still part of it. Valid
   * only right after a call to {@link #parse(String, int, ParseContext)}, it is used to load scripts line by line (issue #5457).
   */
  public boolean isBlockCommentOpen() {
    return blockCommentOpen;
  }

  /**
   * Returns the offset, in the text of the last parse, of the outermost `{` that is still unclosed - or -1 if every brace was
   * matched. Everything from that offset to the end of the text was folded into a single word instead of being split on `;`,
   * since the delimiter branch in {@link #parse(String, int, ParseContext)} requires a brace depth of zero (issue #6439). Valid
   * only right after a call to {@link #parse(String, int, ParseContext)}.
   */
  public int getUnbalancedBraceOffset() {
    return unbalancedBraceOffset;
  }

  /**
   * Sets the language in use, to pick the right line comment marker. Called by the console on `set language = <name>`.
   */
  public void setLanguage(final String language) {
    final boolean sql = language == null || language.toLowerCase(Locale.ENGLISH).startsWith("sql");
    lineComment = sql ? SQL_LINE_COMMENT : OTHER_LINE_COMMENT;
    // THE SQL GRAMMAR READS A LINE COMMENT AS `--` FOLLOWED BY A SPACE, SO `1--2` STAYS ARITHMETIC
    lineCommentNeedsBlank = sql;
    // IF/FOREACH/WHILE BLOCKS ARE SQL SCRIPT: WITH THE OTHER LANGUAGES A BRACE PAIR (A CYPHER `CALL { ... }` SUBQUERY, A MAP
    // LITERAL) IS ALWAYS PART OF A LONGER STATEMENT (ISSUE #8246)
    scriptBlocks = sql;
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

  /**
   * Returns true if the brace pair just closed, opened at {@code openBraceWordOffset} of the current word, is the body of a SQL
   * script block: the word starts with `IF`, `FOREACH` or `WHILE` and the brace follows the closing parenthesis of the block's
   * condition, or the `ELSE` of an `IF`. Only such a block can end a command at its closing brace, since every other brace pair
   * is a literal or a pattern inside a statement that can continue on the next line (issue #8246).
   */
  private boolean isScriptBlockBody(final CharSequence word, final int openBraceWordOffset) {
    if (!scriptBlocks || openBraceWordOffset < 0)
      return false;

    final int start = firstNonBlank(word);
    final boolean ifBlock = startsWithKeyword(word, start, "if");
    if (!ifBlock && !startsWithKeyword(word, start, "foreach") && !startsWithKeyword(word, start, "while"))
      return false;

    int k = openBraceWordOffset - 1;
    while (k > start && Character.isWhitespace(word.charAt(k)))
      --k;
    if (k <= start)
      return false;
    if (word.charAt(k) == ')')
      return true;
    // ONLY AN IF HAS AN ELSE BRANCH IN THE GRAMMAR
    return ifBlock && k - 3 > start && startsWithKeyword(word, k - 3, "else") && !Character.isJavaIdentifierPart(word.charAt(k - 4));
  }

  private static int firstNonBlank(final CharSequence text) {
    int pos = 0;
    while (pos < text.length() && Character.isWhitespace(text.charAt(pos)))
      ++pos;
    return pos;
  }

  /**
   * Returns true if the text at {@code pos} is the given keyword (case-insensitive) as a whole word, i.e. not followed by a
   * letter, a digit or an underscore.
   */
  private static boolean startsWithKeyword(final CharSequence text, final int pos, final String keyword) {
    final int end = pos + keyword.length();
    if (pos < 0 || end > text.length())
      return false;
    for (int k = 0; k < keyword.length(); ++k)
      if (Character.toLowerCase(text.charAt(pos + k)) != keyword.charAt(k))
        return false;
    return end == text.length() || !Character.isJavaIdentifierPart(text.charAt(end));
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
    int openBraceOffset = -1;
    int openBraceWordOffset = -1;
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
        } else {
          // THE ESCAPE CHARACTER IS KEPT: IT IS PART OF THE TEXT HANDED TO THE QUERY ENGINE, WHICH OWNS STRING-LITERAL
          // ESCAPING. isEscaped() ABOVE ALREADY STOPPED THE ESCAPED QUOTE FROM CLOSING THE STRING (ISSUE #6827)
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
      } else {
        if (c == '{') {
          if (braceDepth == 0) {
            openBraceOffset = i;
            openBraceWordOffset = current.length();
          }
          braceDepth++;
          current.append(c);
        } else if (c == '}') {
          final int prevDepth = braceDepth;
          // A CLOSING BRACE WITH NOTHING OPEN IS JUST TEXT: LETTING THE DEPTH GO NEGATIVE WOULD STOP EVERY FOLLOWING SEMICOLON
          // FROM SEPARATING THE COMMANDS FOR THE REST OF THE TEXT (ISSUE #6392)
          if (braceDepth > 0)
            braceDepth--;
          if (braceDepth == 0)
            openBraceOffset = -1;
          current.append(c);

          // A CLOSED SQL SCRIPT BLOCK (`IF (...) { ... }`) ENDS ITS COMMAND EVEN WITH NO SEMICOLON AFTER IT, WHEN MORE CONTENT
          // FOLLOWS ON A NEW LINE. ANY OTHER BALANCED BRACE PAIR - A MAP LITERAL, A `CONTENT { ... }`, A MATCH PATTERN - IS IN THE
          // MIDDLE OF A STATEMENT THAT CAN CONTINUE ON THE NEXT LINE: SPLITTING THERE RAN `UPDATE ... SET x = {json}` WITHOUT ITS
          // WHERE ON A NEW LINE, OVERWRITING EVERY RECORD OF THE TYPE (ISSUE #8246)
          if (prevDepth == 1 && braceDepth == 0 && isScriptBlockBody(current, openBraceWordOffset)) {
            // SKIP BLANKS AND COMMENTS: A COMMENT BETWEEN THE BODY OF AN IF AND ITS ELSE MUST NOT HIDE THE ELSE. A SKIPPED LINE
            // COMMENT ALWAYS ENDS WITH A NEW LINE (OR THE END OF THE TEXT, WHERE NOTHING FOLLOWS TO SPLIT OFF)
            int j = i + 1;
            boolean foundNewline = false;
            while (j < line.length()) {
              final char n = line.charAt(j);
              if (n == '\n' || n == '\r') {
                foundNewline = true;
                j++;
              } else if (Character.isWhitespace(n))
                j++;
              else if (isLineCommentStart(line, j)) {
                while (j < line.length() && line.charAt(j) != '\n' && line.charAt(j) != '\r')
                  j++;
              } else if (isBlockCommentStart(line, j)) {
                final int end = line.indexOf("*/", j + 2);
                j = end < 0 ? line.length() : end + 2;
              } else
                break;
            }

            // THE ELSE BRANCH OF AN IF CONTINUES THE SAME STATEMENT. FOREACH AND WHILE HAVE NO ELSE IN THE GRAMMAR
            final boolean elseOfIf = startsWithKeyword(line, j, "else") && startsWithKeyword(current, firstNonBlank(current), "if");
            if (foundNewline && j < line.length() && !this.isDelimiter(line, j) && !elseOfIf) {
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
    unbalancedBraceOffset = braceDepth > 0 ? openBraceOffset : -1;

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
