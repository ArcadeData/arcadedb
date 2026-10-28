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
package com.arcadedb.postgres;

import com.arcadedb.exception.CommandSQLParsingException;

/**
 * Translates the lexical forms the ArcadeDB SQL lexer reads differently from PostgreSQL: double-quoted identifiers and
 * string literals.
 * <p>
 * <b>Identifiers</b> ({@link #rewrite}). In PostgreSQL (and in the SQL standard) a double-quoted token is always a
 * delimited <b>identifier</b>, while a string literal is single-quoted. ArcadeDB SQL instead inherits the OrientDB
 * dialect where the double quote opens a string literal and the back-tick quotes an identifier. Without this
 * translation a client speaking the Postgres wire protocol (psycopg, the official JDBC driver, Spark, most BI tools)
 * that emits {@code SELECT "name" FROM "Character"} gets back the constant string {@code name} on every row instead of
 * the property value.
 * <p>
 * <b>String literals</b> ({@link #toEngineLiterals}, issue #10871). With {@code standard_conforming_strings} on - its
 * default, and what this server reports at startup - PostgreSQL reads a backslash in a plain {@code '...'} literal as
 * an ordinary character, and only an {@code E'...'} literal (or every literal, with the setting off) processes
 * backslash escapes. The ArcadeDB lexer always processes them, with an escape set of its own, and has no {@code ''}
 * doubling: so {@code 'a\b'} was stored as {@code ab} and {@code 'C:\'} was a syntax error, on a connection told the
 * opposite, and {@code E'...'} did not parse at all. Each literal is therefore read with PostgreSQL's rules and written
 * back in the form the ArcadeDB lexer decodes to the same value. That is done only to the text handed to the SQL
 * engine: the parsers of this module (catalog queries, {@code COPY}) read PostgreSQL's own forms.
 * <p>
 * Both are purely lexical: line comments, block comments, dollar-quoted literals and back-tick identifiers are copied
 * verbatim. The same holds for anything nested in a JSON object or collection literal
 * ({@code INSERT INTO T CONTENT {"k": "v"}}, {@code SET tags = ["a", "b"]}): those constructs only exist in the
 * ArcadeDB dialect, have no Postgres counterpart, and there the double quote keeps its ArcadeDB meaning of string
 * literal. A single-quoted literal keeps its ArcadeDB meaning inside a JSON object only: square brackets are also
 * PostgreSQL's (an {@code ARRAY['a', 'b']} constructor, a subscript), so a literal between them is read as PostgreSQL
 * reads it. Identifiers starting with {@code @} (such as {@code "@rid"}) are left untouched because ArcadeDB SQL
 * already understands that form as a record attribute.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresQuotedIdentifierRewriter {

  private PostgresQuotedIdentifierRewriter() {
  }

  /**
   * {@link #rewrite(String, boolean)} for a connection with {@code standard_conforming_strings} on.
   */
  public static String rewrite(final String sql) {
    return rewrite(sql, true);
  }

  /**
   * Translates the double-quoted identifiers of {@code sql}, leaving its string literals as PostgreSQL wrote them.
   *
   * @param sql                       the statement as received on the wire
   * @param standardConformingStrings whether a backslash in a plain {@code '...'} literal is an ordinary character,
   *                                  which decides where such a literal ends
   *
   * @return the same instance when nothing has to be translated, otherwise the rewritten statement
   */
  public static String rewrite(final String sql, final boolean standardConformingStrings) {
    if (sql == null || sql.indexOf('"') < 0)
      // FAST PATH: no double quote at all, nothing to translate
      return sql;
    return translate(sql, true, false, standardConformingStrings);
  }

  /**
   * Writes every PostgreSQL string literal of {@code sql} in the form the ArcadeDB lexer decodes to the value
   * PostgreSQL reads (issue #10871): with {@code standard_conforming_strings} on, {@code 'a\b'} becomes
   * {@code 'a\\b'}; {@code E'a\tb'} becomes {@code 'a<TAB>b'}; {@code 'it''s'} becomes {@code 'it\'s'}.
   * Double-quoted tokens are left as they are: {@link #rewrite} translates them beforehand when they are identifiers.
   *
   * @return the same instance when no literal has to be translated, otherwise the rewritten statement
   */
  public static String toEngineLiterals(final String sql, final boolean standardConformingStrings) {
    if (sql == null || sql.indexOf('\'') < 0)
      // FAST PATH: no single quote at all, no literal to translate
      return sql;
    final String translated = translate(sql, false, true, standardConformingStrings);
    return translated.equals(sql) ? sql : translated;
  }

  private static String translate(final String sql, final boolean identifiers, final boolean literals,
      final boolean standardConformingStrings) {
    final int len = sql.length();
    final StringBuilder out = new StringBuilder(len + 8);

    int i = 0;
    // '{' opens an ArcadeDB JSON object; '[' an ArcadeDB collection, or a PostgreSQL array constructor or subscript
    int braceDepth = 0;
    int bracketDepth = 0;
    while (i < len) {
      final char c = sql.charAt(i);
      switch (c) {
      case '\'' -> {
        if (braceDepth > 0)
          // INSIDE AN ARCADEDB JSON OBJECT THE LITERAL KEEPS ITS ARCADEDB MEANING
          i = copyEngineStringLiteral(sql, i, len, out);
        else {
          final boolean escapeString = isEscapeStringPrefix(sql, i);
          final boolean escapes = escapeString || !standardConformingStrings;
          i = literals ? translateStringLiteral(sql, i, len, out, escapes, escapeString) : copyStringLiteral(sql, i, len, out, escapes);
        }
      }
      case '$' -> i = copyDollarQuotedLiteral(sql, i, len, out);
      case '-' -> i = copyLineComment(sql, i, len, out);
      case '/' -> i = copyBlockComment(sql, i, len, out);
      case '`' -> i = copyBackTickIdentifier(sql, i, len, out);
      case '"' -> i = !identifiers || braceDepth > 0 || bracketDepth > 0 ?
          // INSIDE A JSON OBJECT OR A COLLECTION LITERAL THE DOUBLE QUOTE KEEPS ITS ARCADEDB MEANING. WHEN ONLY THE
          // LITERALS ARE TRANSLATED, A DOUBLE QUOTE IS WHAT THE IDENTIFIER PASS LEFT: A RECORD ATTRIBUTE OR AN ARCADEDB STRING
          copyJsonString(sql, i, len, out) :
          translateQuotedIdentifier(sql, i, len, out);
      case '{' -> {
        braceDepth++;
        out.append(c);
        i++;
      }
      case '[' -> {
        bracketDepth++;
        out.append(c);
        i++;
      }
      case '}' -> {
        if (braceDepth > 0)
          braceDepth--;
        out.append(c);
        i++;
      }
      case ']' -> {
        if (bracketDepth > 0)
          bracketDepth--;
        out.append(c);
        i++;
      }
      default -> {
        out.append(c);
        i++;
      }
      }
    }

    return out.toString();
  }

  /**
   * Whether the quote at {@code quote} opens a PostgreSQL escape string: it follows an {@code E} that does not end a
   * longer word ({@code E'x'} and {@code (e'x'} are escape strings, {@code WHERE'x'} is not).
   */
  private static boolean isEscapeStringPrefix(final String sql, final int quote) {
    if (quote == 0)
      return false;
    final char prefix = sql.charAt(quote - 1);
    return (prefix == 'E' || prefix == 'e') && (quote == 1 || !isIdentifierPart(sql.charAt(quote - 2)));
  }

  private static boolean isIdentifierPart(final char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  /**
   * The index right after the quote closing the PostgreSQL literal opened at {@code start}, or -1 when it is
   * unterminated. The doubling ({@code ''}) is honoured, and so is a backslash escape when the literal processes them
   * ({@code escapes}): an {@code E'...'} literal, or any literal with {@code standard_conforming_strings} off.
   */
  private static int endOfStringLiteral(final String sql, final int start, final int len, final boolean escapes) {
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && escapes)
        i += 2;
      else if (c == '\'') {
        if (i + 1 < len && sql.charAt(i + 1) == '\'')
          i += 2;
        else
          return i + 1;
      } else
        i++;
    }
    return -1;
  }

  /**
   * Copies a PostgreSQL single-quoted literal verbatim.
   */
  private static int copyStringLiteral(final String sql, final int start, final int len, final StringBuilder out,
      final boolean escapes) {
    final int end = endOfStringLiteral(sql, start, len, escapes);
    // UNTERMINATED: let the parser report the error
    final int copied = end < 0 ? len : end;
    out.append(sql, start, copied);
    return copied;
  }

  /**
   * Reads the PostgreSQL literal opened at {@code start} with PostgreSQL's rules and writes it in the form the
   * ArcadeDB lexer decodes to the same value. An unterminated literal is copied as it is, for the parser to report.
   *
   * @param escapes      whether the literal processes backslash escapes
   * @param escapeString whether it is an {@code E'...'} literal, whose {@code E} is already written and is dropped
   */
  private static int translateStringLiteral(final String sql, final int start, final int len, final StringBuilder out,
      final boolean escapes, final boolean escapeString) {
    final int end = endOfStringLiteral(sql, start, len, escapes);
    if (end < 0) {
      out.append(sql, start, len);
      return len;
    }

    if (escapeString)
      out.setLength(out.length() - 1);

    out.append('\'');
    final int contentEnd = end - 1;
    int i = start + 1;
    while (i < contentEnd) {
      final char c = sql.charAt(i);
      if (c == '\'') {
        // A DOUBLED QUOTE: THE ARCADEDB LEXER HAS NO DOUBLING, A BACKSLASH ESCAPES THE QUOTE THERE
        appendEngineChar('\'', out);
        i += 2;
      } else if (c == '\\' && escapes)
        i = appendEscape(sql, i + 1, contentEnd, out);
      else {
        appendEngineChar(c, out);
        i++;
      }
    }
    out.append('\'');
    return end;
  }

  /**
   * Decodes the PostgreSQL escape whose backslash precedes {@code i} - {@code \b \f \n \r \t}, octal {@code \ooo}, hex
   * {@code \xhh}, a backslash and {@code u} with 4 hex digits or {@code U} with 8, and any other character standing for
   * itself - and writes its value in the engine's form. Returns the index after the escape.
   */
  private static int appendEscape(final String sql, int i, final int end, final StringBuilder out) {
    final char c = sql.charAt(i);
    switch (c) {
    case 'b' -> appendEngineChar('\b', out);
    case 'f' -> appendEngineChar('\f', out);
    case 'n' -> appendEngineChar('\n', out);
    case 'r' -> appendEngineChar('\r', out);
    case 't' -> appendEngineChar('\t', out);
    case '0', '1', '2', '3', '4', '5', '6', '7' -> {
      int value = 0;
      for (int digits = 0; digits < 3 && i < end && sql.charAt(i) >= '0' && sql.charAt(i) <= '7'; digits++, i++)
        value = value * 8 + (sql.charAt(i) - '0');
      appendEngineCodePoint(value & 0xFF, out);
      return i;
    }
    case 'x' -> {
      final int digits = hexDigits(sql, i + 1, end, 2);
      if (digits == 0)
        // NOT FOLLOWED BY A HEX DIGIT: THE LETTER STANDS FOR ITSELF, AS ANY OTHER ESCAPED CHARACTER DOES
        appendEngineChar(c, out);
      else {
        appendEngineCodePoint(Integer.parseInt(sql, i + 1, i + 1 + digits, 16), out);
        return i + 1 + digits;
      }
    }
    case 'u', 'U' -> {
      // EXACTLY FOUR, OR EIGHT, HEX DIGITS: ANYTHING ELSE IS REFUSED, AS POSTGRESQL REFUSES IT
      final int wanted = c == 'u' ? 4 : 8;
      final long codePoint = hexDigits(sql, i + 1, end, wanted) == wanted ? Long.parseLong(sql, i + 1, i + 1 + wanted, 16) : -1;
      // a 4-digit surrogate is half of a pair spelled as two escapes, which the string holds as they are; an 8-digit one names nothing
      if (codePoint < 0 || codePoint > Character.MAX_CODE_POINT || (c == 'U' && codePoint >= Character.MIN_SURROGATE
          && codePoint <= Character.MAX_SURROGATE))
        throw new CommandSQLParsingException(
            "invalid Unicode escape in a string literal: Unicode escapes must be a backslash and u followed by 4 hex digits, or "
                + "U followed by 8, naming a valid code point");
      appendEngineCodePoint((int) codePoint, out);
      return i + 1 + wanted;
    }
    default -> appendEngineChar(c, out);
    }
    return i + 1;
  }

  private static int hexDigits(final String sql, final int from, final int end, final int max) {
    int digits = 0;
    while (digits < max && from + digits < end && Character.digit(sql.charAt(from + digits), 16) >= 0)
      digits++;
    return digits;
  }

  private static void appendEngineCodePoint(final int codePoint, final StringBuilder out) {
    if (Character.isBmpCodePoint(codePoint))
      appendEngineChar((char) codePoint, out);
    else
      out.appendCodePoint(codePoint);
  }

  /**
   * Writes one character of a literal's value so that the ArcadeDB lexer decodes it back to itself: the backslash and
   * the quote are escaped, and so are the line breaks, which that lexer does not accept inside a literal. Every other
   * character stands for itself.
   */
  private static void appendEngineChar(final char c, final StringBuilder out) {
    switch (c) {
    case '\\' -> out.append("\\\\");
    case '\'' -> out.append("\\'");
    case '\n' -> out.append("\\n");
    case '\r' -> out.append("\\r");
    default -> out.append(c);
    }
  }

  /**
   * Copies a single-quoted literal of the ArcadeDB dialect verbatim, honouring the back-slash escape of its lexer while
   * scanning for the closing quote.
   */
  private static int copyEngineStringLiteral(final String sql, final int start, final int len, final StringBuilder out) {
    out.append('\'');
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        out.append(c).append(sql.charAt(i + 1));
        i += 2;
      } else if (c == '\'') {
        out.append('\'');
        return i + 1;
      } else {
        out.append(c);
        i++;
      }
    }
    // UNTERMINATED: let the parser report the error
    return i;
  }

  /**
   * Copies a dollar-quoted literal ({@code $$...$$} or {@code $tag$...$tag$}) verbatim. When no matching closing tag is
   * found the dollar sign is treated as an ordinary character, so ArcadeDB context variables such as {@code $current}
   * are not affected.
   */
  private static int copyDollarQuotedLiteral(final String sql, final int start, final int len, final StringBuilder out) {
    int i = start + 1;
    while (i < len && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_'))
      i++;

    if (i >= len || sql.charAt(i) != '$') {
      out.append('$');
      return start + 1;
    }

    final String tag = sql.substring(start, i + 1);
    final int end = sql.indexOf(tag, i + 1);
    if (end < 0) {
      out.append('$');
      return start + 1;
    }

    out.append(sql, start, end + tag.length());
    return end + tag.length();
  }

  private static int copyLineComment(final String sql, final int start, final int len, final StringBuilder out) {
    if (start + 1 >= len || sql.charAt(start + 1) != '-') {
      out.append('-');
      return start + 1;
    }

    int i = start;
    while (i < len && sql.charAt(i) != '\n')
      i++;
    out.append(sql, start, i);
    return i;
  }

  private static int copyBlockComment(final String sql, final int start, final int len, final StringBuilder out) {
    if (start + 1 >= len || sql.charAt(start + 1) != '*') {
      out.append('/');
      return start + 1;
    }

    int i = start + 2;
    int depth = 1;
    while (i < len && depth > 0) {
      if (i + 1 < len && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        depth++;
        i += 2;
      } else if (i + 1 < len && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        depth--;
        i += 2;
      } else
        i++;
    }
    out.append(sql, start, i);
    return i;
  }

  private static int copyBackTickIdentifier(final String sql, final int start, final int len, final StringBuilder out) {
    out.append('`');
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        out.append(c).append(sql.charAt(i + 1));
        i += 2;
      } else if (c == '`') {
        out.append('`');
        return i + 1;
      } else {
        out.append(c);
        i++;
      }
    }
    return i;
  }

  /**
   * Copies a JSON string verbatim, honouring the back-slash escapes of the JSON grammar.
   */
  private static int copyJsonString(final String sql, final int start, final int len, final StringBuilder out) {
    out.append('"');
    int i = start + 1;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '\\' && i + 1 < len) {
        out.append(c).append(sql.charAt(i + 1));
        i += 2;
      } else if (c == '"') {
        out.append('"');
        return i + 1;
      } else {
        out.append(c);
        i++;
      }
    }
    // UNTERMINATED: let the parser report the error
    return i;
  }

  private static int translateQuotedIdentifier(final String sql, final int start, final int len, final StringBuilder out) {
    final StringBuilder identifier = new StringBuilder();
    int i = start + 1;
    boolean closed = false;
    while (i < len) {
      final char c = sql.charAt(i);
      if (c == '"') {
        if (i + 1 < len && sql.charAt(i + 1) == '"') {
          // ESCAPED DOUBLE QUOTE INSIDE THE IDENTIFIER
          identifier.append('"');
          i += 2;
        } else {
          i++;
          closed = true;
          break;
        }
      } else {
        identifier.append(c);
        i++;
      }
    }

    if (!closed || identifier.isEmpty() || identifier.charAt(0) == '@') {
      // UNTERMINATED, EMPTY OR RECORD ATTRIBUTE (E.G. "@rid"): LEAVE THE ORIGINAL TEXT UNCHANGED
      out.append(sql, start, i);
      return i;
    }

    out.append('`');
    for (int k = 0; k < identifier.length(); k++) {
      final char c = identifier.charAt(k);
      // inside a back-tick identifier a backslash escapes the character that follows it, so both characters have to be escaped:
      // leaving a backslash bare would make the engine read `My\Type` as MyType, and one before the closing back-tick would
      // consume it
      if (c == '`' || c == '\\')
        out.append('\\');
      out.append(c);
    }
    out.append('`');
    return i;
  }
}
