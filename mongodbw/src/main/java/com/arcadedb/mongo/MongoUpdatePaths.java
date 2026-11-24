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

import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.util.Arrays;
import java.util.List;

/**
 * The dotted paths of an update: how many fields one may name, and which of the paths of a statement conflict. Neither copies a
 * prefix of a path: the memory they use is that of the paths themselves, and the time they take is that of comparing the paths.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class MongoUpdatePaths {
  /** The most fields a path may name: the nesting limit of the documents mongod stores. */
  static final int MAX_DEPTH = 100;

  private MongoUpdatePaths() {
  }

  /**
   * Refuses a path that names more than {@link #MAX_DEPTH} fields, as a statement-level {@code BadValue}.
   */
  static void checkDepth(final String path) {
    int fields = 1;
    for (int dot = path.indexOf('.'); dot >= 0; dot = path.indexOf('.', dot + 1))
      if (++fields > MAX_DEPTH)
        throw new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(),
            "An update path can name at most " + MAX_DEPTH + " nested fields");
  }

  /**
   * Two of {@code paths} of which one is the other or lies below it, as {@code {path, at}}: the path met last in the list and the
   * one it conflicts with (the path itself when it is the other's duplicate or ancestor), or {@code null} when there are none.
   * <p>
   * The paths are sorted field by field, the {@code '.'} before any other character, so that the paths below one follow it
   * directly: only neighbours are compared.
   */
  static String[] findConflict(final List<String> paths) {
    final int size = paths.size();
    if (size < 2)
      return null;

    final String[] sorted = paths.toArray(new String[0]);
    Arrays.sort(sorted, MongoUpdatePaths::compare);
    for (int i = 1; i < size; i++) {
      final String shorter = sorted[i - 1];
      final String longer = sorted[i];
      if (isSameOrBelow(longer, shorter))
        return paths.indexOf(shorter) > paths.indexOf(longer) ? new String[] { shorter, shorter } : new String[] { longer, shorter };
    }
    return null;
  }

  /** Orders two paths field by field: the {@code '.'} sorts before any other character, and a path before the ones below it. */
  static int compare(final String a, final String b) {
    final int length = Math.min(a.length(), b.length());
    for (int i = 0; i < length; i++) {
      final char x = a.charAt(i);
      final char y = b.charAt(i);
      if (x != y)
        return x == '.' ? -1 : y == '.' ? 1 : Character.compare(x, y);
    }
    return Integer.compare(a.length(), b.length());
  }

  private static boolean isSameOrBelow(final String path, final String other) {
    return path.startsWith(other) && (path.length() == other.length() || path.charAt(other.length()) == '.');
  }
}
