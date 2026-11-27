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
package com.arcadedb.mongo;

/**
 * The text a client sends (a command, a database, a collection or a mechanism name) as it is shown in an error message and in a
 * log line: printable characters only, and no longer than {@link #MAX_LENGTH}. A control character (a line break, an escape
 * sequence) is replaced by {@code ?}, so a text stays on one line.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class MongoWireText {
  /** The longest name of a client shown in full: a name longer than this is cut and ends with an ellipsis. */
  static final int MAX_LENGTH = 128;

  /** The longest error message shown in a log line: it holds names of the client, and the words of the server around them. */
  static final int MAX_MESSAGE_LENGTH = 512;

  private static final String ELLIPSIS = "...";

  /** The ASCII characters between the space and the tilde are printable; the ones before are control characters, DEL follows. */
  private static final int FIRST_PRINTABLE      = 0x20;
  private static final int LAST_ASCII_PRINTABLE = 0x7E;

  /** The line and the paragraph separators of Unicode: not control characters, and they end a line all the same. */
  private static final int LINE_SEPARATOR      = 0x2028;
  private static final int PARAGRAPH_SEPARATOR = 0x2029;

  private MongoWireText() {
  }

  /**
   * {@code text} with every character that is not printable replaced, and cut at {@link #MAX_LENGTH}. The text itself is returned
   * when there is nothing to change, so the usual case allocates nothing.
   */
  static String clean(final String text) {
    return clean(text, MAX_LENGTH);
  }

  /** The same, cut at {@code maxLength} instead. */
  static String clean(final String text, final int maxLength) {
    if (text == null)
      return "null";

    final int length = text.length();
    int end = Math.min(length, maxLength);
    // never cut between the two halves of a surrogate pair
    if (end < length && end > 0 && Character.isHighSurrogate(text.charAt(end - 1)))
      end--;

    StringBuilder cleaned = null;
    for (int i = 0; i < end; ) {
      // a code point, not a char: a supplementary character is two chars, and the invisible ones are among them
      final int codePoint = text.codePointAt(i);
      final int next = i + Character.charCount(codePoint);
      if (isPrintable(codePoint)) {
        if (cleaned != null)
          cleaned.appendCodePoint(codePoint);
      } else {
        if (cleaned == null)
          cleaned = new StringBuilder(end + ELLIPSIS.length()).append(text, 0, i);
        cleaned.append('?');
      }
      i = next;
    }

    if (end == length)
      return cleaned == null ? text : cleaned.toString();

    if (cleaned == null)
      cleaned = new StringBuilder(end + ELLIPSIS.length()).append(text, 0, end);
    return cleaned.append(ELLIPSIS).toString();
  }

  /**
   * What a request carries under a key that holds a name, shown the same way: a text as {@link #clean(String)} shows it, and any
   * other value (a document, an array) as the name of its type, because printing it would show the content of the request.
   */
  static String describe(final Object value) {
    if (value == null)
      return "null";
    return value instanceof String text ? clean(text) : value.getClass().getSimpleName();
  }

  /**
   * A copy of {@code throwable} for a log record: the same stack frames, with the message of each throwable of the chain shown as
   * {@link #clean(String, int)} shows it. The log prints a throwable as it is, so the original would print the messages of the
   * exceptions it wraps as they are.
   */
  static Throwable forLog(final Throwable throwable) {
    return throwable == null ? null : new LoggedThrowable(throwable, 0);
  }

  private static boolean isPrintable(final int codePoint) {
    if (codePoint >= FIRST_PRINTABLE && codePoint <= LAST_ASCII_PRINTABLE)
      return true;
    if (Character.isISOControl(codePoint) || codePoint == LINE_SEPARATOR || codePoint == PARAGRAPH_SEPARATOR)
      return false;
    // the invisible formatting characters (zero width, bidirectional controls, tags) change what a line looks like
    return Character.getType(codePoint) != Character.FORMAT;
  }

  private static final class LoggedThrowable extends RuntimeException {
    private static final long serialVersionUID = 1L;
    /** The causes copied: a chain longer than this, or one that loops, is cut. */
    private static final int  MAX_CAUSES       = 8;

    LoggedThrowable(final Throwable original, final int depth) {
      super(summaryOf(original),
          depth < MAX_CAUSES && original.getCause() != null ? new LoggedThrowable(original.getCause(), depth + 1) : null, false, true);
      setStackTrace(original.getStackTrace());
    }

    private static String summaryOf(final Throwable original) {
      final String name = original.getClass().getName();
      return original.getMessage() == null ? name : name + ": " + clean(original.getMessage(), MAX_MESSAGE_LENGTH);
    }
  }
}
