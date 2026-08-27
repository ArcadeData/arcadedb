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
/* ParserGeneratorCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.graphql.parser;

public class StringValue extends AbstractValue {
  protected String val;

  /**
   * Memoised result of {@link #getValue()}. The computation is idempotent, so the unsynchronised publication of an
   * immutable String is a benign race: a second thread either sees null and recomputes the same value, or sees the
   * value already computed.
   */
  private transient String decoded;

  public StringValue(final int id) {
    super(id);
  }

  @Override
  public String toString() {
    return "StringValue{" + val + '}';
  }

  /**
   * Returns the string with the surrounding quotes removed and the GraphQL escape sequences decoded. The lexer
   * accepts the whole escape set (a backslash followed by one of {@code " \ / b f n r t}, or by {@code u} and four
   * hexadecimal digits) but used to keep the raw token image, so the backslash sequences reached the database
   * verbatim instead of the characters they stand for. See issue #6836.
   */
  public String getValue() {
    if (val == null)
      return null;

    String result = decoded;
    if (result == null) {
      final String unquoted =
          val.length() > 1 && val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"' ?
              val.substring(1, val.length() - 1) :
              val;
      decoded = result = unescape(unquoted);
    }
    return result;
  }

  /**
   * Decodes the GraphQL escape sequences of an already unquoted string literal. Returns the argument itself when
   * there is nothing to decode, so the common case allocates nothing.
   */
  private static String unescape(final String text) {
    final int firstEscape = text.indexOf('\\');
    if (firstEscape < 0)
      return text;

    final int length = text.length();
    final StringBuilder buffer = new StringBuilder(length);
    buffer.append(text, 0, firstEscape);

    for (int i = firstEscape; i < length; ++i) {
      final char c = text.charAt(i);
      if (c != '\\' || i == length - 1) {
        buffer.append(c);
        continue;
      }

      final char escaped = text.charAt(++i);
      switch (escaped) {
      case '"' -> buffer.append('"');
      case '\\' -> buffer.append('\\');
      case '/' -> buffer.append('/');
      case 'b' -> buffer.append('\b');
      case 'f' -> buffer.append('\f');
      case 'n' -> buffer.append('\n');
      case 'r' -> buffer.append('\r');
      case 't' -> buffer.append('\t');
      case 'u' -> {
        if (i + 4 < length) {
          final int codePoint = parseHex(text, i + 1);
          if (codePoint >= 0) {
            buffer.append((char) codePoint);
            i += 4;
            continue;
          }
        }
        // NOT A WELL-FORMED UNICODE ESCAPE: THE LEXER CANNOT PRODUCE ONE, SO KEEP THE TEXT RATHER THAN DESTROY IT
        buffer.append('\\').append(escaped);
      }
      // THE LEXER ONLY ADMITS THE ESCAPES ABOVE: ANYTHING ELSE IS LEFT UNTOUCHED INSTEAD OF BEING SWALLOWED
      default -> buffer.append('\\').append(escaped);
      }
    }

    return buffer.toString();
  }

  /**
   * Parses the 4 hexadecimal digits starting at {@code offset}, returning -1 when any of them is not a hex digit.
   */
  private static int parseHex(final String text, final int offset) {
    int value = 0;
    for (int i = offset; i < offset + 4; ++i) {
      final int digit = Character.digit(text.charAt(i), 16);
      if (digit < 0)
        return -1;
      value = (value << 4) | digit;
    }
    return value;
  }
}
/* ParserGeneratorCC - OriginalChecksum=20e0a8fec30917a1654cd45e385fe65f (do not edit this line) */
