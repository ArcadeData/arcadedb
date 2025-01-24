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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;

public class QueryOperatorEquals {
  public static boolean equals(Object left, Object right) {
    if (left == null || right == null)
      return false;

    if (left == right)
      return true;

    if (left.getClass().equals(right.getClass()))
      // SAME TYPE, NO CONVERSION
      BinaryComparator.equals(left, right);

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
      final Number[] couple = Type.castComparableNumber(number, number1);
      return couple[0].equals(couple[1]);
    }

    // ALL OTHER CASES
    try {
      right = Type.convert(null, right, left.getClass());
      if (right == null)
        return false;
      if (left instanceof byte[] bytes && right instanceof byte[] bytes1) {
        return Arrays.equals(bytes, bytes1);
      }
      return BinaryComparator.equals(left, right);
    } catch (final Exception ignore) {
      return false;
    }
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
              if (o != null && o.equals(value))
                return true;
            }
          }

          return fieldValue.equals(value);
        }
      }
      return false;
    }

    if (value instanceof String string && RID.is(value))
      value = new RID(other.getDatabase(), string);

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
            if (o != null && o.equals(iValue))
              return true;
          }
        }

        return fieldValue.equals(iValue);
      }
    }
    return false;

  }
}
