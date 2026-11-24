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
    for (int i = 0; i < end; i++) {
      final char c = text.charAt(i);
      if (isPrintable(c)) {
        if (cleaned != null)
          cleaned.append(c);
      } else {
        if (cleaned == null)
          cleaned = new StringBuilder(end + ELLIPSIS.length()).append(text, 0, i);
        cleaned.append('?');
      }
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

  private static boolean isPrintable(final char c) {
    if (c >= ' ' && c < 0x7F)
      return true;
    if (Character.isISOControl(c) || c == ' ' || c == ' ')
      return false;
    // the invisible formatting characters (zero width, bidirectional controls) change what a line looks like
    return Character.getType(c) != Character.FORMAT;
  }
}
