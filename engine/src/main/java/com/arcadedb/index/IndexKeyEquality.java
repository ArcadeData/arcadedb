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
   * <p>
   * Which is what {@link Arrays#deepEquals} already is, element for element: it short-circuits on identity and on a
   * null operand, checks the lengths, and compares each element the way {@link #sameValue} does.
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
    // -0.0 and 0.0 are one key to the index comparator (issue #8920), and Double.equals() reads them as two
    if (a instanceof Double da && b instanceof Double db)
      return da.equals(db) || (da == 0.0d && db == 0.0d);
    if (a instanceof Float fa && b instanceof Float fb)
      return fa.equals(fb) || (fa == 0.0f && fb == 0.0f);
    return Objects.deepEquals(a, b);
  }

  /**
   * The hash that goes with {@link #sameTuple}: content-derived for array-valued elements, so two content-equal keys
   * cannot land in different buckets of a {@code HashMap} while comparing equal. A null tuple hashes to 0, which is
   * what {@link Arrays#deepHashCode} answers for it.
   */
  public static int hashTuple(final Object[] tuple) {
    if (tuple == null)
      return 0;
    int result = 1;
    for (final Object element : tuple)
      result = 31 * result + hashValue(element);
    return result;
  }

  /** Same as the element contribution of {@link Arrays#deepHashCode}, except that -0.0 hashes as 0.0 (issue #8920). */
  private static int hashValue(final Object element) {
    if (element == null)
      return 0;
    if (element instanceof Double d)
      return Double.hashCode(d + 0.0d);
    if (element instanceof Float f)
      return Float.hashCode(f + 0.0f);
    // an array element (BINARY, a vector) hashes by content as stored, as Arrays.deepHashCode does, without a wrapper
    if (element instanceof byte[] a)
      return Arrays.hashCode(a);
    if (element instanceof float[] a)
      return Arrays.hashCode(a);
    if (element instanceof double[] a)
      return Arrays.hashCode(a);
    if (element instanceof int[] a)
      return Arrays.hashCode(a);
    if (element instanceof long[] a)
      return Arrays.hashCode(a);
    if (element instanceof short[] a)
      return Arrays.hashCode(a);
    if (element instanceof char[] a)
      return Arrays.hashCode(a);
    if (element instanceof boolean[] a)
      return Arrays.hashCode(a);
    if (element instanceof Object[] a)
      return Arrays.deepHashCode(a);
    return element.hashCode();
  }
}
