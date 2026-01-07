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
package com.arcadedb.utility;

import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;

public class CollectionUtils {

  public static int compare(final List l1, final List l2) {
    final int length = Math.min(l1.size(), l2.size());

    for (int j = 0; j < length; j++) {
      final int cmp = BinaryComparator.compareTo(l1.get(j), l2.get(j));
      if (cmp != 0)
        return cmp;
    }

    if (l1.size() > l2.size())
      return 1;
    else if (l1.size() < l2.size())
      return -1;
    return 0;
  }

  public static int compare(final Map<?, Comparable> m1, final Map<?, Comparable> m2) {
    final Set<? extends Map.Entry<?, Comparable>> entries1 = m1.entrySet();
    for (Map.Entry<?, Comparable> entry : entries1) {
      final Comparable value1 = entry.getValue();
      final Comparable value2 = m2.get(entry.getKey());
      if (value1 == null) {
        if (value2 == null)
          return 0;
        return -1;
      } else if (value2 == null)
        return 1;

      final int cmp = value1.compareTo(value2);
      if (cmp != 0)
        return cmp;
    }

    if (m1.size() > m2.size())
      return 1;
    else if (m1.size() < m2.size())
      return -1;
    return 0;
  }

  /**
   * Returns the count of the remaining items that have not been iterated yet.<br>
   * <b>NOTE: the default implementation consumes the iterator</b>.
   */
  public static long countEntries(final Iterator iterator) {
    long tot = 0;

    while (iterator.hasNext()) {
      iterator.next();
      tot++;
    }

    return tot;
  }

  public static List<Document> resultsetToListOfDocuments(final ResultSet resultset) {
    final List<Document> list = new ArrayList<>();
    while (resultset.hasNext())
      list.add(resultset.next().toElement());
    return list;
  }

  public static Document getFirstResultAsDocument(final ResultSet resultset) {
    if (resultset.hasNext())
      return resultset.next().toElement();
    return null;
  }

  public static Object getFirstResultValue(final ResultSet resultset, final String propertyName) {
    if (resultset.hasNext())
      return resultset.next().getProperty(propertyName);
    return null;
  }

  public static <T> List<T> addToUnmodifiableList(List<T> list, T objToAdd) {
    final ArrayList<T> result = new ArrayList<>(list.size() + 1);
    result.addAll(list);
    result.add(objToAdd);
    return Collections.unmodifiableList(result);
  }

  public static <T> List<T> removeFromUnmodifiableList(List<T> list, T objToRemove) {
    final ArrayList<T> result = new ArrayList<>(list.size() - 1);
    for (int i = 0; i < list.size(); i++) {
      final T o = list.get(i);
      if (Objects.equals(o, objToRemove))
        continue;
      result.add(o);
    }
    return Collections.unmodifiableList(result);
  }

  public static <T> List<T> addAllToUnmodifiableList(List<T> list, List<T> objsToAdd) {
    final Set<T> result = new HashSet<>(list.size() + objsToAdd.size());
    result.addAll(list);
    result.addAll(objsToAdd);
    return Collections.unmodifiableList(new ArrayList(result));
  }

  public static <T> List<T> removeAllFromUnmodifiableList(List<T> list, List<T> objsToRemove) {
    final ArrayList<T> result = new ArrayList<>(list.size() - objsToRemove.size());
    for (int i = 0; i < list.size(); i++) {
      final T o = list.get(i);

      boolean found = false;
      for (int k = 0; k < objsToRemove.size(); k++) {
        if (Objects.equals(o, objsToRemove.get(k))) {
          found = true;
          break;
        }
      }
      if (!found)
        result.add(o);
    }
    return Collections.unmodifiableList(result);
  }


  /**
   * Converts any array (including primitive arrays) to a List.
   * Handles the special case of primitive arrays which cannot be cast to Object[].
   */
  public static List<?> arrayToList(final Object array) {
    if (!array.getClass().isArray()) {
      throw new IllegalArgumentException("Input must be an array");
    }

    final Class<?> componentType = array.getClass().getComponentType();

    // Handle primitive arrays specially
    if (componentType.isPrimitive()) {
      if (componentType == float.class) {
        final float[] floatArray = (float[]) array;
        final List<Float> list = new ArrayList<>(floatArray.length);
        for (float f : floatArray) {
          list.add(f);
        }
        return list;
      } else if (componentType == int.class) {
        final int[] intArray = (int[]) array;
        final List<Integer> list = new ArrayList<>(intArray.length);
        for (int i : intArray) {
          list.add(i);
        }
        return list;
      } else if (componentType == double.class) {
        final double[] doubleArray = (double[]) array;
        final List<Double> list = new ArrayList<>(doubleArray.length);
        for (double d : doubleArray) {
          list.add(d);
        }
        return list;
      } else if (componentType == long.class) {
        final long[] longArray = (long[]) array;
        final List<Long> list = new ArrayList<>(longArray.length);
        for (long l : longArray) {
          list.add(l);
        }
        return list;
      } else if (componentType == boolean.class) {
        final boolean[] boolArray = (boolean[]) array;
        final List<Boolean> list = new ArrayList<>(boolArray.length);
        for (boolean b : boolArray) {
          list.add(b);
        }
        return list;
      } else if (componentType == byte.class) {
        final byte[] byteArray = (byte[]) array;
        final List<Byte> list = new ArrayList<>(byteArray.length);
        for (byte b : byteArray) {
          list.add(b);
        }
        return list;
      } else if (componentType == char.class) {
        final char[] charArray = (char[]) array;
        final List<Character> list = new ArrayList<>(charArray.length);
        for (char c : charArray) {
          list.add(c);
        }
        return list;
      } else if (componentType == short.class) {
        final short[] shortArray = (short[]) array;
        final List<Short> list = new ArrayList<>(shortArray.length);
        for (short s : shortArray) {
          list.add(s);
        }
        return list;
      }
    }

    // For object arrays, use Arrays.asList
    return Arrays.asList((Object[]) array);
  }

}
