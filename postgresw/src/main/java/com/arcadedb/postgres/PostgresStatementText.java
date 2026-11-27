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

/**
 * What the parsers of a statement's text agree on, for the ones that read it character by character rather than with a regular
 * expression: a statement is the client's text, up to the 32 KB the wire takes, and may be made of little else than blanks, so
 * each of them reads it once, front to back.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class PostgresStatementText {
  private PostgresStatementText() {
  }

  /** What the regular expression {@code \s} reads: space, tab, line feed, vertical tab, form feed and carriage return. */
  static boolean isBlank(final char c) {
    return c == ' ' || c >= '\t' && c <= '\r';
  }

  /** The first position at or after {@code from} that is not a blank, which is the length of {@code text} when there is none. */
  static int skipBlanks(final String text, final int from) {
    int i = from;
    while (i < text.length() && isBlank(text.charAt(i)))
      i++;
    return i;
  }
}
