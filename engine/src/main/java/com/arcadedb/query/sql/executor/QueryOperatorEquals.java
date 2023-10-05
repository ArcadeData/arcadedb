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

    if (left instanceof Result && !(right instanceof Result)) {
      if (((Result) left).isElement()) {
        left = ((Result) left).toElement();
      } else {
        return comparesValues(right, (Result) left, true);
      }
    }

    if (right instanceof Result && !(left instanceof Result)) {
      if (((Result) right).isElement()) {
        right = ((Result) right).toElement();
      } else {
        return comparesValues(left, (Result) right, true);
      }
    }

    // RECORD & ORID
    if (left instanceof Identifiable)
      return comparesValues(right, (Identifiable) left, true);
    else if (right instanceof Identifiable)
      return comparesValues(left, (Identifiable) right, true);
    else if (right instanceof Result)
      return comparesValues(left, (Result) right, true);

    // NUMBERS
    if (left instanceof Number && right instanceof Number) {
      final Number[] couple = Type.castComparableNumber((Number) left, (Number) right);
      return couple[0].equals(couple[1]);
    }

    // ALL OTHER CASES
    try {
      right = Type.convert(null, right, left.getClass());
      if (right == null)
        return false;
      if (left instanceof byte[] && right instanceof byte[]) {
        return Arrays.equals((byte[]) left, (byte[]) right);
      }
      return BinaryComparator.equals(left, right);
    } catch (final Exception ignore) {
      return false;
    }
  }

  protected static boolean comparesValues(Object value, final Identifiable record, final boolean iConsiderIn) {
    // ORID && RECORD
    final RID other = record.getIdentity();

    if (record instanceof Document && record.getIdentity() == null) {
      // DOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final Set<String> firstFieldName = ((Document) record).getPropertyNames();
      if (!firstFieldName.isEmpty()) {
        final Object fieldValue = ((Document) record).get(firstFieldName.iterator().next());
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

    if (value instanceof String && RID.is(value))
      value = new RID(other.getDatabase(), (String) value);

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
