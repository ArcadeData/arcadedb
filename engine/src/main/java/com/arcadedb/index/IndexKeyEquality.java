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
package com.arcadedb.index;

import java.util.Arrays;
import java.util.Objects;

/**
 * The one "are these the same index key?" policy, shared by every side of the index path.
 * <p>
 * An index key is an {@code Object[]} whose elements may THEMSELVES be arrays: a {@code BINARY} property is a
 * {@code byte[]} and a dense vector key a {@code float[]}, and neither overrides {@code Object.equals()}. A plain
 * {@link Arrays#equals(Object[], Object[])} therefore compares those elements by IDENTITY, so two independently
 * deserialised but content-equal keys read as different keys.
 * <p>
 * Two places used to answer this question, and they disagreed on exactly those keys. {@code DocumentIndexer} was
 * routed through {@link Objects#deepEquals} under issue #7109, so a record update no longer saw a spurious key
 * change; the transaction-side overlay ({@code TransactionIndexContext}) was left on the shallow form, while its own
 * {@code ComparableKey.compareTo} compared the same elements BY CONTENT through {@code BinaryComparator}. The
 * {@code TreeMap} of pending entries and the {@code HashMap} inside each of its values then held different opinions
 * about the same key: the in-transaction duplicate check on a UNIQUE {@code BINARY} index did not fire, and the
 * {@code REMOVE}-then-{@code ADD} merge lost the {@code oldRid} that commit keys the removal of the superseded RID
 * on (issue #7881).
 * <p>
 * Kept here, next to the indexes, so a third side cannot pick a fourth notion of equality.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IndexKeyEquality {
  private IndexKeyEquality() {
  }

  /**
   * Two key tuples are the same key when they have the same length and every element is
   * {@link #sameValue(Object, Object) the same key value}. A null tuple equals only another null tuple.
   */
  public static boolean sameTuple(final Object[] a, final Object[] b) {
    if (a == b)
      return true;
    if (a == null || b == null || a.length != b.length)
      return false;
    for (int i = 0; i < a.length; i++)
      if (!sameValue(a[i], b[i]))
        return false;
    return true;
  }

  /**
   * Content-aware equality for a single key value. {@link Objects#deepEquals} uses {@code Object.equals()} for a
   * scalar, the element-wise {@code Arrays.equals()} overload of the matching primitive array type for a primitive
   * array ({@code byte[]}, {@code float[]}, ...) and {@code Arrays.deepEquals()} for an {@code Object[]}.
   */
  public static boolean sameValue(final Object a, final Object b) {
    return Objects.deepEquals(a, b);
  }

  /**
   * The hash that goes with {@link #sameTuple}: content-derived for array-valued elements, so two content-equal keys
   * cannot land in different buckets of a {@code HashMap} while comparing equal. A null tuple hashes to 0, which is
   * what {@link Arrays#deepHashCode} answers for it.
   */
  public static int hashTuple(final Object[] tuple) {
    return Arrays.deepHashCode(tuple);
  }
}
