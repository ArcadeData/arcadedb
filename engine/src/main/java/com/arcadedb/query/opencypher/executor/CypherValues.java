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
package com.arcadedb.query.opencypher.executor;

/**
 * Comparisons between a value stored on a record and a value a query supplies.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherValues {
  private CypherValues() {
  }

  /**
   * Numeric-tolerant equality: a stored {@code Integer} 1 equals a supplied {@code Long} 1, which is what makes a
   * MERGE pattern match a record written with a different integral width. A {@code Float} and a {@code Double} may
   * still report unequal after widening (0.1f != 0.1d); that is conservative on purpose, since every caller pays for
   * a false negative with a redundant write or an extra probe, never with a wrong answer.
   */
  public static boolean equalValues(final Object a, final Object b) {
    if (a == null)
      return b == null;
    if (a.equals(b))
      return true;
    if (a instanceof Number numberA && b instanceof Number numberB)
      return numberA.longValue() == numberB.longValue()
          && Double.compare(numberA.doubleValue(), numberB.doubleValue()) == 0;
    return false;
  }
}
