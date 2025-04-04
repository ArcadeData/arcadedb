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

import com.arcadedb.database.Identifiable;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.IterableObject;
import com.arcadedb.utility.IterableObjectArray;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.ResettableIterator;

import java.lang.reflect.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.StreamSupport;

/**
 * Handles Multi-value types such as Arrays, Collections and Maps. It recognizes special Arcade collections.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
@SuppressWarnings("unchecked")
public class MultiValue {

  /**
   * Checks if a class is a multi-value type.
   *
   * @param iType Class to check
   *
   * @return true if it's an array, a collection or a map, otherwise false
   */
  public static boolean isMultiValue(final Class<?> iType) {
    return Collection.class.isAssignableFrom(iType) || Map.class.isAssignableFrom(iType) || MultiIterator.class.isAssignableFrom(
        iType) || (Iterable.class.isAssignableFrom(iType) && !Identifiable.class.isAssignableFrom(iType)) || iType.isArray()
        || ResultSet.class.isAssignableFrom(iType);
  }

  /**
   * Checks if the object is a multi-value type.
   *
   * @param iObject Object to check
   *
   * @return true if it's an array, a collection or a map, otherwise false
   */
  public static boolean isMultiValue(final Object iObject) {
    return iObject != null && isMultiValue(iObject.getClass());
  }

  public static boolean isIterable(final Object iObject) {
    return iObject instanceof Iterable<?> || iObject instanceof Iterator<?>;
  }

  /**
   * Returns the estimated size of the multi-value object.
   *
   * @param object Multi-value object (array, collection or map)
   *
   * @return the size of the multi value object
   */
  public static int getSize(final Object object) {
    if (object == null)
      return 0;

    if (!isMultiValue(object))
      return 0;

    if (object instanceof Collection<?> collection)
      return collection.size();
    else if (object instanceof Map<?, ?> map)
      return map.size();
    else if (object.getClass().isArray())
      return Array.getLength(object);
    else if (object instanceof ResettableIterator<?> resettableIterator)
      return (int) resettableIterator.countEntries();
    else if (object instanceof Iterable<?> iterable)
      return (int) StreamSupport.stream(iterable.spliterator(), false).count();
    else if (object instanceof Iterator<?> iterator)
      return (int) CollectionUtils.countEntries(iterator);

    return 0;
  }

  /**
   * Returns the size of the multi-value object only if the size is available without computing.
   *
   * @param object Multi-value object (array, collection or map)
   *
   * @return the size of the multi value object, -1 if the size is not available without computing
   */
  public static int getSizeIfAvailable(final Object object) {
    if (object == null)
      return 0;

    if (object instanceof Collection<?> collection)
      return collection.size();
    else if (object instanceof Map<?, ?> map)
      return map.size();
    else if (object.getClass().isArray())
      return Array.getLength(object);
    return -1;
  }

  /**
   * Returns the first item of the Multi-value object (array, collection or map)
   *
   * @param object Multi-value object (array, collection or map)
   *
   * @return The first item if any
   */
  public static Object getFirstValue(final Object object) {
    if (object == null)
      return null;

    if (!isMultiValue(object))
      return null;

    if (getSizeIfAvailable(object) == 0)
      return null;

    try {
      if (object instanceof List<?> list)
        return list.get(0);
      else if (object instanceof Iterable<?> iterable)
        return iterable.iterator().next();
      else if (object instanceof Map<?, ?> map)
        return map.values().iterator().next();
      else if (object.getClass().isArray())
        return Array.get(object, 0);
    } catch (final RuntimeException e) {
      // IGNORE IT
      LogManager.instance()
          .log(object, Level.FINE, "Error on reading the first item of the Multi-value field '%s'", null, object, e);
    }

    return null;
  }

  /**
   * Returns the last item of the Multi-value object (array, collection or map)
   *
   * @param object Multi-value object (array, collection or map)
   *
   * @return The last item if any
   */
  public static Object getLastValue(final Object object) {
    if (object == null)
      return null;

    if (!isMultiValue(object))
      return null;

    if (getSizeIfAvailable(object) == 0)
      return null;

    try {
      if (object instanceof List<?> list)
        return list.get(list.size() - 1);
      else if (object instanceof Iterable<?> iterable) {
        Object last = null;
        for (final Object o : iterable)
          last = o;
        return last;
      } else if (object instanceof Map<?, ?> map) {
        Object last = null;
        for (final Object o : map.values())
          last = o;
        return last;
      } else if (object.getClass().isArray())
        return Array.get(object, Array.getLength(object) - 1);
    } catch (final RuntimeException e) {
      // IGNORE IT
      LogManager.instance()
          .log(object, Level.FINE, "Error on reading the last item of the Multi-value field '%s'", null, object, e);
    }

    return null;
  }

  /**
   * Returns the iIndex item of the Multi-value object (array, collection or map)
   *
   * @param iObject Multi-value object (array, collection or map)
   * @param iIndex  integer as the position requested
   *
   * @return The first item if any
   */
  public static Object getValue(final Object iObject, final int iIndex) {
    if (iObject == null)
      return null;

    if (!isMultiValue(iObject))
      return null;

    final int size = MultiValue.getSizeIfAvailable(iObject);

    if (size > -1 && !(iObject instanceof Iterator) && iIndex > size)
      return null;

    try {
      if (iObject instanceof List<?> list)
        return (list.get(iIndex));
      else if (iObject instanceof Set<?> set) {
        int i = 0;
        for (final Object o : set) {
          if (i++ == iIndex) {
            return o;
          }
        }
      } else if (iObject instanceof Map<?, ?> map) {
        int i = 0;
        for (final Object o : map.values()) {
          if (i++ == iIndex) {
            return o;
          }
        }
      } else if (iObject.getClass().isArray()) {
        if (iIndex >= Array.getLength(iObject))
          return null;
        return Array.get(iObject, iIndex);
      } else if (iObject instanceof Iterator<?> || iObject instanceof Iterable<?>) {

        final Iterator<Object> it = (iObject instanceof Iterable<?>) ?
            ((Iterable<Object>) iObject).iterator() :
            (Iterator<Object>) iObject;
        if (it.hasNext()) {
          for (int i = 0; it.hasNext(); ++i) {
            final Object o = it.next();
            if (i == iIndex) {
              if (it instanceof ResettableIterator<?> resettableIterator)
                resettableIterator.reset();
              return o;
            }
          }

          if (it instanceof ResettableIterator<?> resettableIterator)
            resettableIterator.reset();
        }
      }
    } catch (final RuntimeException e) {
      // IGNORE IT
      LogManager.instance()
          .log(iObject, Level.FINE, "Error on reading the first item of the Multi-value field '%s'", null, iObject, e);
    }
    return null;
  }

  /**
   * Sets the value of the Multi-value object (array or collection) at index
   *
   * @param multiValue Multi-value object (array, collection)
   * @param value      The value to set at this specified index.
   * @param index      integer as the position requested
   */
  public static void setValue(final Object multiValue, final Object value, final int index) {
    if (multiValue instanceof List list) {
      list.set(index, value);
    } else if (multiValue.getClass().isArray()) {
      Array.set(multiValue, index, value);
    } else {
      throw new IllegalArgumentException("Can only set positional indices for Lists and Arrays");
    }
  }

  /**
   * Returns an {@literal Iterable<Object>} object to browse the multi-value instance (array, collection or map).
   *
   * @param iObject Multi-value object (array, collection or map)
   */
  public static Iterable<?> getMultiValueIterable(final Object iObject) {
    if (iObject == null)
      return null;

    if (iObject instanceof Iterable<?> iterable)
      return iterable;
    else if (iObject instanceof Collection<?> collection)
      return collection;
    else if (iObject instanceof Map<?, ?> map)
      return map.values();
    else if (iObject.getClass().isArray())
      return new IterableObjectArray<>(iObject);
    else if (iObject instanceof Iterator<?> iterator) {

      final List<Object> temp = new ArrayList<>();
      for (final Iterator<?> it = iterator; it.hasNext(); )
        temp.add(it.next());
      return temp;
    }

    return new IterableObject<>(iObject);
  }

  /**
   * Returns an {@literal Iterable<Object>} object to browse the multi-value instance (array, collection or map).
   *
   * @param iObject             Multi-value object (array, collection or map)
   * @param iForceConvertRecord allow to force settings to convert RIDs to records while browsing.
   */
  public static Iterable<Object> getMultiValueIterable(final Object iObject, final boolean iForceConvertRecord) {
    if (iObject == null)
      return null;

    if (iObject instanceof Iterable<?>)
      return (Iterable<Object>) iObject;
    else if (iObject instanceof Collection<?>)
      return ((Collection<Object>) iObject);
    else if (iObject instanceof Map<?, ?>)
      return ((Map<?, Object>) iObject).values();
    else if (iObject.getClass().isArray())
      return new IterableObjectArray<>(iObject);
    else if (iObject instanceof Iterator<?>) {
      final List<Object> temp = new ArrayList<>();
      for (final Iterator<Object> it = (Iterator<Object>) iObject; it.hasNext(); )
        temp.add(it.next());
      return temp;
    }

    return new IterableObject<>(iObject);
  }

  /**
   * Returns an {@literal Iterator<Object>} object to browse the multi-value instance (array, collection or map)
   *
   * @param iObject             Multi-value object (array, collection or map)
   * @param iForceConvertRecord allow to force settings to convert RIDs to records while browsing.
   */

  public static Iterator<?> getMultiValueIterator(final Object iObject, final boolean iForceConvertRecord) {
    if (iObject == null)
      return null;

    if (iObject instanceof Iterator<?> iterator)
      return iterator;

    if (iObject instanceof Iterable<?> iterable)
      return iterable.iterator();
    if (iObject instanceof Map<?, ?> map)
      return map.values().iterator();
    if (iObject.getClass().isArray())
      return new IterableObjectArray<>(iObject).iterator();

    return new IterableObject<>(iObject);
  }

  /**
   * Returns an {@literal Iterator<Object>} object to browse the multi-value instance (array, collection or map)
   *
   * @param iObject Multi-value object (array, collection or map)
   */

  public static Iterator<?> getMultiValueIterator(final Object iObject) {
    if (iObject == null)
      return null;

    if (iObject instanceof Iterator<?> iterator)
      return iterator;

    if (iObject instanceof Iterable<?> iterable)
      return iterable.iterator();
    if (iObject instanceof Map<?, ?> map)
      return map.values().iterator();
    if (iObject.getClass().isArray())
      return new IterableObjectArray<>(iObject).iterator();

    return new IterableObject<>(iObject);
  }

  /**
   * Returns a stringified version of the multi-value object.
   *
   * @param iObject Multi-value object (array, collection or map)
   *
   * @return a stringified version of the multi-value object.
   */
  public static String toString(final Object iObject) {
    final StringBuilder sb = new StringBuilder(2048);

    if (iObject instanceof Iterable<?>) {
      final Iterable<Object> coll = (Iterable<Object>) iObject;

      sb.append('[');
      for (final Iterator<Object> it = coll.iterator(); it.hasNext(); ) {
        try {
          final Object e = it.next();
          sb.append(e == iObject ? "(this Collection)" : e);
          if (it.hasNext())
            sb.append(", ");
        } catch (final NoSuchElementException ignore) {
          // IGNORE THIS
        }
      }
      return sb.append(']').toString();
    } else if (iObject instanceof Map<?, ?>) {
      final Map<String, Object> map = (Map<String, Object>) iObject;

      Map.Entry<String, Object> e;

      sb.append('{');
      for (final Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext(); ) {
        try {
          e = it.next();

          sb.append(e.getKey());
          sb.append(":");
          sb.append(e.getValue() == iObject ? "(this Map)" : e.getValue());
          if (it.hasNext())
            sb.append(", ");
        } catch (final NoSuchElementException ignore) {
          // IGNORE THIS
        }
      }
      return sb.append('}').toString();
    }

    return iObject.toString();
  }

  /**
   * Utility function that add a value to the main object. It takes care about collections/array and single values.
   *
   * @param iObject MultiValue where to add value(s)
   * @param iToAdd  Single value, array of values or collections of values. Map are not supported.
   *
   * @return
   */
  public static Object add(final Object iObject, final Object iToAdd) {
    if (iObject != null) {
      if (iObject instanceof Collection<?>) {
        final Collection coll = (Collection) iObject;

        if (!(iToAdd instanceof Map) && isMultiValue(iToAdd)) {
          // COLLECTION - COLLECTION
          for (final Object o : getMultiValueIterable(iToAdd, false)) {
            if (!(o instanceof Map) && isMultiValue(o))
              add(coll, o);
            else
              coll.add(o);
          }
        } else if (iToAdd != null && iToAdd.getClass().isArray()) {
          // ARRAY - COLLECTION
          for (int i = 0; i < Array.getLength(iToAdd); ++i) {
            final Object o = Array.get(iToAdd, i);
            if (!(o instanceof Map) && isMultiValue(o))
              add(coll, o);
            else
              coll.add(o);
          }

        } else if (iToAdd instanceof Map<?, ?>) {
          // MAP
          coll.add(iToAdd);
        } else if (iToAdd instanceof Iterator<?>) {
          // ITERATOR
          for (final Iterator<?> it = (Iterator<?>) iToAdd; it.hasNext(); )
            coll.add(it.next());
        } else
          coll.add(iToAdd);

      } else if (iObject.getClass().isArray()) {
        // ARRAY - ?

        final Object[] copy;
        if (iToAdd instanceof Collection<?>) {
          // ARRAY - COLLECTION
          final int tot = Array.getLength(iObject) + ((Collection<Object>) iToAdd).size();
          copy = Arrays.copyOf((Object[]) iObject, tot);
          final Iterator<Object> it = ((Collection<Object>) iToAdd).iterator();
          for (int i = Array.getLength(iObject); i < tot; ++i)
            copy[i] = it.next();

        } else if (iToAdd != null && iToAdd.getClass().isArray()) {
          // ARRAY - ARRAY
          final int tot = Array.getLength(iObject) + Array.getLength(iToAdd);
          copy = Arrays.copyOf((Object[]) iObject, tot);
          System.arraycopy(iToAdd, 0, iObject, Array.getLength(iObject), Array.getLength(iToAdd));

        } else {
          copy = Arrays.copyOf((Object[]) iObject, Array.getLength(iObject) + 1);
          copy[copy.length - 1] = iToAdd;
        }
        return copy;
      }
    }

    return iObject;
  }

  /**
   * Utility function that remove a value from the main object. It takes care about collections/array and single values.
   *
   * @param iObject         MultiValue where to add value(s)
   * @param iToRemove       Single value, array of values or collections of values. Map are not supported.
   * @param iAllOccurrences True if the all occurrences must be removed or false of only the first one (Like
   *                        java.util.Collection.remove())
   *
   * @return
   */
  public static Object remove(Object iObject, Object iToRemove, final boolean iAllOccurrences) {
    if (iObject != null) {
      if (iObject instanceof MultiIterator<?>) {
        final Collection<Object> list = new LinkedList<>();
        for (final Object o : ((MultiIterator<?>) iObject))
          list.add(o);
        iObject = list;
      }

      if (iToRemove instanceof MultiIterator<?>) {
        // TRANSFORM IN SET ONCE TO OPTIMIZE LOOPS DURING REMOVE
        final Set<Object> set = new HashSet<>();
        for (final Object o : ((MultiIterator<?>) iToRemove))
          set.add(o);
        iToRemove = set;
      }

      if (iObject instanceof Collection<?>) {
        final Collection coll = (Collection) iObject;

        if (iToRemove instanceof Collection<?>) {
          // COLLECTION - COLLECTION
          for (final Object o : (Collection<Object>) iToRemove) {
            if (isMultiValue(o))
              remove(coll, o, iAllOccurrences);
            else
              removeFromCollection(iObject, coll, o, iAllOccurrences);
          }
        } else if (iToRemove != null && iToRemove.getClass().isArray()) {
          // ARRAY - COLLECTION
          for (int i = 0; i < Array.getLength(iToRemove); ++i) {
            final Object o = Array.get(iToRemove, i);
            if (isMultiValue(o))
              remove(coll, o, iAllOccurrences);
            else
              removeFromCollection(iObject, coll, o, iAllOccurrences);
          }

        } else if (iToRemove instanceof Map<?, ?>) {
          // MAP
          for (final Map.Entry<Object, Object> entry : ((Map<Object, Object>) iToRemove).entrySet())
            coll.remove(entry.getKey());
        } else if (iToRemove instanceof Iterator<?>) {
          // ITERATOR
          if (iToRemove instanceof MultiIterator<?>)
            ((MultiIterator<?>) iToRemove).reset();

          if (iAllOccurrences) {
            final Collection<Object> collection = (Collection) iObject;
            final MultiIterator<?> it = (MultiIterator<?>) iToRemove;
            batchRemove(collection, it);
          } else {
            final Iterator<?> it = (Iterator<?>) iToRemove;
            if (it.hasNext()) {
              final Object itemToRemove = it.next();
              coll.remove(itemToRemove);
            }
          }
        } else
          removeFromCollection(iObject, coll, iToRemove, iAllOccurrences);

      } else if (iObject.getClass().isArray()) {
        // ARRAY - ?

        final Object[] copy;
        if (iToRemove instanceof Collection<?>) {
          // ARRAY - COLLECTION
          final int sourceTot = Array.getLength(iObject);
          final int tot = sourceTot - ((Collection<Object>) iToRemove).size();
          copy = new Object[tot];

          int k = 0;
          for (int i = 0; i < sourceTot; ++i) {
            final Object o = Array.get(iObject, i);
            if (o != null) {
              boolean found = false;
              for (final Object toRemove : (Collection<Object>) iToRemove) {
                if (o.equals(toRemove)) {
                  // SKIP
                  found = true;
                  break;
                }
              }

              if (!found)
                copy[k++] = o;
            }
          }

        } else if (iToRemove != null && iToRemove.getClass().isArray()) {
          throw new UnsupportedOperationException("Cannot execute remove() against an array");

        } else {
          throw new UnsupportedOperationException("Cannot execute remove() against an array");
        }
        return copy;

      } else if (iObject instanceof Map) {
        ((Map) iObject).remove(iToRemove);
      } else
        throw new IllegalArgumentException("Object " + iObject + " is not a multi value");
    }

    return iObject;
  }

  protected static void removeFromCollection(final Object iObject, final Collection<Object> coll, final Object iToRemove,
      final boolean iAllOccurrences) {
    if (iAllOccurrences && !(iObject instanceof Set)) {
      // BROWSE THE COLLECTION ONE BY ONE TO REMOVE ALL THE OCCURRENCES
      coll.removeIf(iToRemove::equals);
    } else
      coll.remove(iToRemove);

  }

  private static void batchRemove(final Collection<Object> coll, final Iterator<?> it) {
    int approximateRemainingSize = -1;

    while (it.hasNext()) {
      final Set<?> batch = prepareBatch(it, approximateRemainingSize);
      coll.removeAll(batch);
      approximateRemainingSize -= batch.size();
    }
  }

  private static Set<?> prepareBatch(final Iterator<?> it, final int approximateRemainingSize) {
    final HashSet<Object> batch;
    if (approximateRemainingSize > -1) {
      if (approximateRemainingSize > 10000)
        batch = new HashSet<>(13400);
      else
        batch = new HashSet<>((int) (approximateRemainingSize / 0.75));
    } else {
      batch = new HashSet<>();
    }

    int count = 0;
    while (count < 10000 && it.hasNext()) {
      batch.add(it.next());
      count++;
    }

    return batch;
  }

  public static Object[] array(final Object iValue) {
    return array(iValue, Object.class);
  }

  public static <T> T[] array(final Object iValue, final Class<? extends T> iClass) {
    return array(iValue, iClass, null);
  }

  public static <T> T[] array(final Object iValue, final Class<? extends T> iClass, final Callable<Object, Object> iCallback) {
    if (iValue == null)
      return null;

    final T[] result;

    if (isMultiValue(iValue)) {
      // CREATE STATIC ARRAY AND FILL IT
      result = (T[]) Array.newInstance(iClass, getSize(iValue));
      int i = 0;
      for (final Iterator<T> it = (Iterator<T>) getMultiValueIterator(iValue, false); it.hasNext(); ++i)
        result[i] = (T) convert(it.next(), iCallback);
    } else if (isIterable(iValue)) {
      // SIZE UNKNOWN: USE A LIST AS TEMPORARY OBJECT
      final List<T> temp = new ArrayList<>();
      for (final Iterator<T> it = (Iterator<T>) getMultiValueIterator(iValue, false); it.hasNext(); )
        temp.add((T) convert(it.next(), iCallback));

      if (iClass.equals(Object.class))
        result = (T[]) temp.toArray();
      else
        // CONVERT THEM
        result = temp.toArray((T[]) Array.newInstance(iClass, getSize(iValue)));

    } else {
      result = (T[]) Array.newInstance(iClass, 1);
      result[0] = (T) convert(iValue, iCallback);
    }

    return result;
  }

  public static Object convert(final Object iObject, final Callable<Object, Object> iCallback) {
    return iCallback != null ? iCallback.call(iObject) : iObject;
  }

  public static boolean equals(final Collection<Object> col1, final Collection<Object> col2) {
    if (col1.size() != col2.size())
      return false;
    return col1.containsAll(col2) && col2.containsAll(col1);
  }

  public static boolean contains(final Object iObject, final Object iItem) {
    if (iObject == null)
      return false;

    if (iObject instanceof Collection)
      return ((Collection) iObject).contains(iItem);

    else if (iObject.getClass().isArray()) {
      final int size = Array.getLength(iObject);
      for (int i = 0; i < size; ++i) {
        final Object item = Array.get(iObject, i);
        if (item != null && item.equals(iItem))
          return true;
      }
    }

    return false;
  }

  public static int indexOf(final Object iObject, final Object iItem) {
    if (iObject == null)
      return -1;

    if (iObject instanceof List)
      return ((List) iObject).indexOf(iItem);

    else if (iObject.getClass().isArray()) {
      final int size = Array.getLength(iObject);
      for (int i = 0; i < size; ++i) {
        final Object item = Array.get(iObject, i);
        if (item != null && item.equals(iItem))
          return i;
      }
    }

    return -1;
  }

  public static Object toSet(final Object o) {
    if (o instanceof Set<?>)
      return o;
    else if (o instanceof Collection<?>)
      return new HashSet<Object>((Collection<?>) o);
    else if (o instanceof Map<?, ?>) {
      final Collection values = ((Map) o).values();
      return values instanceof Set ? values : new HashSet(values);
    } else if (o.getClass().isArray()) {
      final HashSet set = new HashSet();
      final int tot = Array.getLength(o);
      for (int i = 0; i < tot; ++i) {
        set.add(Array.get(o, i));
      }
      return set;
    } else if (o instanceof MultiValue) {
    } else if (o instanceof Iterator<?>) {
      final HashSet set = new HashSet();
      while (((Iterator<?>) o).hasNext()) {
        set.add(((Iterator<?>) o).next());
      }

      return set;
    } else if (o instanceof Iterable && !(o instanceof Identifiable)) {
      final Iterator iterator = ((Iterable) o).iterator();
      final Set result = new HashSet();
      while (iterator.hasNext()) {
        result.add(iterator.next());
      }
      return result;
    }

    final HashSet set = new HashSet(1);
    set.add(o);
    return set;
  }

  public static <T> List<T> getSingletonList(final T item) {
    final List<T> list = new ArrayList<>(1);
    list.add(item);
    return list;
  }
}
