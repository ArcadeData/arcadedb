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
package com.arcadedb.query.sql.executor;

public class QueryHelper {
  protected static final char WILDCARD_ANYCHAR = '?';
  protected static final char WILDCARD_ANY     = '%';

  public static boolean like(String currentValue, String value) {
    if (currentValue == null || value == null || (currentValue.isEmpty() ^ value.isEmpty()))
      // EMPTY/NULL PARAMETERS
      return false;
    else if (currentValue.isEmpty() && value.isEmpty())
      return true;

    value = convertForRegExp(value);
    return currentValue.matches(value);
  }

  public static String convertForRegExp(String value) {

    final StringBuilder sb = new StringBuilder("(?s)");

    for (int i = 0; i < value.length();++i) {

      final char c = value.charAt(i);
      switch (c) {
      case '\\':
        if (i + 1 < value.length()) {
          final char next = value.charAt(i + 1);
          if (next == WILDCARD_ANY) {
            sb.append(next);
            ++i;
            break;
          } else if (next == WILDCARD_ANYCHAR) {
            sb.append("\\" + next);
            ++i;
            break;
          }
        }
      case '[':
      case ']':
      case '{':
      case '}':
      case '(':
      case ')':
      case '|':
      case '*':
      case '+':
      case '$':
      case '^':
      case '.':
        sb.append("\\" + c);
        break;
      case WILDCARD_ANYCHAR:
        sb.append(".");
        break;
      case WILDCARD_ANY:
        sb.append(".*");
        break;
      default:
        sb.append(c);
        break;
      }
    }
    return sb.toString();
  }
}
