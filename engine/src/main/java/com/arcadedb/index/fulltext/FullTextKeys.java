/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import java.util.List;

/**
 * The keys a full-text index stores its postings under (issue #12162).
 * <p>
 * A one-property index stores every token as it is. A multi-property index stores every token twice: under
 * {@code property:token}, the key of a query qualified to that property, and unprefixed, the key of an unqualified query. A token
 * can itself read {@code property:...}: the StandardTokenizer keeps {@code title:java} as one token, since a colon between letters
 * does not split a word. Stored as it is, the unprefixed posting of that token would be the {@code title} posting of {@code java}.
 * Such a token is stored behind one {@link #ESCAPE}, and so is a token that would otherwise read as an escaped one, so the three
 * kinds of keys never meet: a qualified key starts with a property name, an escaped one with the escape, and every other key is
 * an unprefixed token as it is.
 * <p>
 * An index written before the escape kept those few tokens unescaped: they read as qualified keys until the index is rebuilt.
 */
final class FullTextKeys {
  static final char ESCAPE = ':';

  private FullTextKeys() {
  }

  /** Whether the index stores qualified keys beside the unprefixed ones, so that unprefixed tokens can need the escape. */
  static boolean qualifies(final List<String> propertyNames) {
    return propertyNames != null && propertyNames.size() > 1;
  }

  /** The key the unprefixed posting of {@code token} is stored under. */
  static String unqualifiedKey(final List<String> propertyNames, final String token) {
    return qualifies(propertyNames) && needsEscape(propertyNames, token, 0) ? ESCAPE + token : token;
  }

  /** The token an unqualified query finds under {@code key}, or null when {@code key} is the key of a qualified posting. */
  static String unqualifiedToken(final List<String> propertyNames, final String key) {
    if (!qualifies(propertyNames))
      return key;
    if (!key.isEmpty() && key.charAt(0) == ESCAPE && needsEscape(propertyNames, key, 1))
      return key.substring(1);
    return isQualified(propertyNames, key, 0) ? null : key;
  }

  /** Whether every character of {@code text} is the escape: then the keys that start with it include the escaped ones. */
  static boolean onlyEscapes(final String text) {
    for (int i = 0; i < text.length(); i++)
      if (text.charAt(i) != ESCAPE)
        return false;
    return true;
  }

  /** {@code token.substring(from)} reads as a qualified key, or as an escaped one. */
  private static boolean needsEscape(final List<String> propertyNames, final String token, final int from) {
    for (int i = from; i < token.length(); i++) {
      if (isQualified(propertyNames, token, i))
        return true;
      if (token.charAt(i) != ESCAPE)
        return false;
    }
    return false;
  }

  /** {@code key.substring(from)} starts with the name of an indexed property and a colon. */
  private static boolean isQualified(final List<String> propertyNames, final String key, final int from) {
    for (final String property : propertyNames) {
      final int end = from + property.length();
      if (end < key.length() && key.charAt(end) == ':' && key.startsWith(property, from))
        return true;
    }
    return false;
  }
}
