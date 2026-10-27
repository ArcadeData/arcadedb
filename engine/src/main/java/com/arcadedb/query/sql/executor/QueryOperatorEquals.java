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

import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.DateUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public class QueryOperatorEquals {
  public static boolean equals(Object left, Object right) {
    if (left == null || right == null)
      return false;

    if (left == right)
      return true;

    // A java.util.Calendar (a supported arcadedb.dateImplementation / dateTimeImplementation, or a parameter) is its
    // instant on either side, as the index key and BinaryComparator.compareTo read it. Its runtime class GregorianCalendar
    // is no conversion target, so a scan of a DATE column answered nothing where the index answered the row, and
    // Calendar.equals() also compares the zone, so two Calendars at one instant were not equal (issue #9647)
    if (left instanceof Calendar calendar)
      left = calendar.getTime();
    if (right instanceof Calendar calendar)
      right = calendar.getTime();

    // The same for a subclass of java.util.Date (java.sql.Timestamp, java.sql.Date) against a value of another class: it is no
    // conversion target either, so a Timestamp on the left of "=" was never equal to the same instant as a LocalDateTime or a
    // Date that the same value on the right matched, and the answer depended on the side the parameter stood on (issue #10595).
    // A pair of one class is left alone: a Timestamp keeps its nanoseconds against another Timestamp
    if (left instanceof Date date && !date.getClass().equals(Date.class) && !date.getClass().equals(right.getClass()))
      left = new Date(date.getTime());
    if (right instanceof Date date && !date.getClass().equals(Date.class) && !date.getClass().equals(left.getClass()))
      right = new Date(date.getTime());

    // Two lists or two maps - of one implementation or of two (a stored ArrayList against an immutable List.of() parameter) - are
    // compared as they are, not converted into one another first, and element by element under THIS rule: two values "=" reports
    // equal are equal as elements of a list or a map too (issues #10448, #10595). The comparator's own element rule knows numbers,
    // BigDecimal scale and one temporal pair, and nothing else: the instant of a stored LocalDateTime was not equal to the same
    // instant as a java.util.Date, an Instant, a Calendar or a Timestamp inside a list, and "<>" answered the row. This arm stands
    // before the same-class one for that reason: two ArrayLists are a same-class pair. It takes every collection and every array but
    // a byte[] (a BINARY scalar), not only a java.util.List: a Set fell past a List-only arm to the conversion below and was compared
    // with the comparator's element rule (issue #10736)
    if (isSequence(left) && isSequence(right))
      return sequencesEqual(left, right);
    if (left instanceof Map<?, ?> map && right instanceof Map<?, ?> map1)
      return BinaryComparator.mapsEqual(map, map1, QueryOperatorEquals::elementsEqual);

    if (left.getClass().equals(right.getClass()))
      // SAME TYPE, NO CONVERSION
      return BinaryComparator.equals(left, right);

    // EmbeddedDocument has null identity; the Identifiable branch below would misroute it as a sub-query result.
    if (left instanceof EmbeddedDocument && right instanceof EmbeddedDocument)
      return BinaryComparator.equals(left, right);

    if (left instanceof Result result && !(right instanceof Result)) {
      if (result.isElement()) {
        left = result.toElement();
      } else {
        return comparesValues(right, result, true);
      }
    }

    if (right instanceof Result result && !(left instanceof Result)) {
      if (result.isElement()) {
        right = result.toElement();
      } else {
        return comparesValues(left, result, true);
      }
    }

    // RECORD & ORID
    if (left instanceof Identifiable identifiable)
      return comparesValues(right, identifiable, true);
    else if (right instanceof Identifiable identifiable)
      return comparesValues(left, identifiable, true);
    else if (right instanceof Result result)
      return comparesValues(left, result, true);

    // NUMBERS
    if (left instanceof Number number && right instanceof Number number1) {
      return Type.numbersEqual(number, number1);
    }

    // A NUMBER AGAINST A DATETIME: EPOCH MILLISECONDS, EXACTLY, ON WHICHEVER SIDE THE NUMBER STANDS (ISSUE #10560)
    final int order = DateUtils.compareNumberAndTemporal(left, right);
    if (order != DateUtils.NOT_NUMBER_AND_TEMPORAL)
      return order == 0;

    // ALL OTHER CASES
    try {
      // convertOrNull(): comparing a value against a literal of an incompatible shape is an ordinary "not equal",
      // not a query to refuse, so this stays on the lenient side of the strict/lenient split issue #8090 drew.
      // A DATE against a datetime is that day at midnight, not the datetime narrowed to its day (issue #10206)
      left = DateUtils.dateAsMidnightAgainst(left, right);
      right = Type.convertOrNull(null, right, left.getClass());
      if (right == null)
        return false;
      if (left instanceof byte[] bytes && right instanceof byte[] bytes1) {
        return Arrays.equals(bytes, bytes1);
      }
      // A conversion into a collection (a value into the list it is compared with) is compared by the collection arms above, so
      // the comparator's own element rule is never reached with a pair of collections (issue #10736)
      if ((isSequence(left) && isSequence(right)) || (left instanceof Map && right instanceof Map))
        return equals(left, right);
      return BinaryComparator.equals(left, right);
    } catch (final Exception ignore) {
      return false;
    }
  }

  /** A collection, or an array that is a sequence of values: any array but a {@code byte[]}, which is a BINARY scalar. */
  private static boolean isSequence(final Object value) {
    return value instanceof Collection<?> || MultiValue.isSequenceArray(value);
  }

  /**
   * Two collections or arrays under the element rule of {@code =}: two sets hold no order and are equal when their elements pair off,
   * any other pair is compared in iteration order, a set against a list in the order the set iterates, as when it was converted into
   * a list before (issue #10736).
   */
  private static boolean sequencesEqual(final Object left, final Object right) {
    if (left instanceof Set<?> set && right instanceof Set<?> set1)
      return BinaryComparator.setsEqual(set, set1, QueryOperatorEquals::elementsEqual);
    return BinaryComparator.listsEqual(asCollection(left), asCollection(right), QueryOperatorEquals::elementsEqual);
  }

  private static Collection<?> asCollection(final Object value) {
    return value instanceof Collection<?> collection ? collection : CollectionUtils.arrayToList(value);
  }

  /** A pair of elements of two lists or maps: {@code =} itself, except that {@code null} is equal to {@code null} as it is in a list. */
  private static boolean elementsEqual(final Object left, final Object right) {
    if (left == null || right == null)
      return left == null && right == null;
    // Two strings, or two numbers of one class, are most of the elements of a list: nothing above converts them, so they are
    // compared as the same-class arm of equals() would, without the checks for a Calendar, a Date subclass and a collection
    if (left.getClass() == right.getClass() && (left instanceof String || left instanceof Number))
      return BinaryComparator.equals(left, right);
    return equals(left, right);
  }

  // "=" semantics, tried both ways like the list form of IN (the other side is converted to the class of the column, or the column to
  // the class of the other side). Links keep plain equals() so a link never recurses (#9031); an Identifiable or Result on the other side
  // terminates too: a record with an identity goes straight to RID equality and a Result cannot form a cycle.
  private static boolean valueEquals(final Object fieldValue, final Object other) {
    if (fieldValue.equals(other))
      return true;
    if (fieldValue instanceof Identifiable || fieldValue instanceof Result)
      return false;
    // same class or two numbers: the comparison is symmetric, so the reverse call would repeat the one that just failed
    if (other == null)
      return false;
    if (fieldValue.getClass() == other.getClass() || (fieldValue instanceof Number && other instanceof Number))
      return equals(fieldValue, other);
    return equals(other, fieldValue) || equals(fieldValue, other);
  }

  protected static boolean comparesValues(Object value, final Identifiable record, final boolean iConsiderIn) {
    // ORID && RECORD
    final RID other = record.getIdentity();

    if (record instanceof Document document && record.getIdentity() == null) {
      // DOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final Set<String> firstFieldName = document.getPropertyNames();
      if (!firstFieldName.isEmpty()) {
        final Object fieldValue = document.get(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && MultiValue.isMultiValue(fieldValue)) {
            for (final Object o : MultiValue.getMultiValueIterable(fieldValue, false)) {
              if (o != null && valueEquals(o, value))
                return true;
            }
          }

          return valueEquals(fieldValue, value);
        }
      }
      return false;
    }

    if (value instanceof String string && RID.is(value))
      value = new RID(string);

    return other.equals(value);
  }

  protected static boolean comparesValues(final Object iValue, final Result iRecord, final boolean iConsiderIn) {
    if (iRecord.isElement()) {
      return comparesValues(iValue, iRecord.getElement().get(), iConsiderIn);
    }

    if (iRecord.equals(iValue)) {
      return true;
    }
    // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
    final Set<String> firstFieldName = iRecord.getPropertyNames();
    if (!firstFieldName.isEmpty()) {
      final Object fieldValue = iRecord.getProperty(firstFieldName.iterator().next());
      if (fieldValue != null) {
        if (iConsiderIn && MultiValue.isMultiValue(fieldValue)) {
          for (final Object o : MultiValue.getMultiValueIterable(fieldValue, false)) {
            if (o != null && valueEquals(o, iValue))
              return true;
          }
        }

        return valueEquals(fieldValue, iValue);
      }
    }
    return false;

  }
}
