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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.schema.Type;

import java.util.Arrays;
import java.util.Set;

public class QueryOperatorEquals {
  public static boolean equals(Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null)
      return false;

    if (iLeft == iRight) {
      return true;
    }

    if (iLeft instanceof Result && !(iRight instanceof Result)) {
      if (((Result) iLeft).isElement()) {
        iLeft = ((Result) iLeft).toElement();
      } else {
        return comparesValues(iRight, (Result) iLeft, true);
      }
    }

    if (iRight instanceof Result && !(iLeft instanceof Result)) {
      if (((Result) iRight).isElement()) {
        iRight = ((Result) iRight).toElement();
      } else {
        return comparesValues(iLeft, (Result) iRight, true);
      }
    }

    // RECORD & ORID
    if (iLeft instanceof Record)
      return comparesValues(iRight, (Record) iLeft, true);
    else if (iRight instanceof Record)
      return comparesValues(iLeft, (Record) iRight, true);
    else if (iRight instanceof Result) {
      return comparesValues(iLeft, (Result) iRight, true);
    }

    // NUMBERS
    if (iLeft instanceof Number && iRight instanceof Number) {
      Number[] couple = Type.castComparableNumber((Number) iLeft, (Number) iRight);
      return couple[0].equals(couple[1]);
    }

    // ALL OTHER CASES
    try {
      final Object right = Type.convert(null, iRight, iLeft.getClass());

      if (right == null)
        return false;
      if (iLeft instanceof byte[] && iRight instanceof byte[]) {
        return Arrays.equals((byte[]) iLeft, (byte[]) iRight);
      }
      return iLeft.equals(right);
    } catch (Exception ignore) {
      return false;
    }
  }

  protected static boolean comparesValues(final Object iValue, final Record iRecord, final boolean iConsiderIn) {
    // ORID && RECORD
    final RID other = iRecord.getIdentity();

    if (iRecord instanceof Document && iRecord.getIdentity() == null) {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final Set<String> firstFieldName = ((Document) iRecord).getPropertyNames();
      if (!firstFieldName.isEmpty()) {
        Object fieldValue = ((Document) iRecord).get(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && MultiValue.isMultiValue(fieldValue)) {
            for (Object o : MultiValue.getMultiValueIterable(fieldValue, false)) {
              if (o != null && o.equals(iValue))
                return true;
            }
          }

          return fieldValue.equals(iValue);
        }
      }
      return false;
    }
    return other.equals(iValue);
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
      Object fieldValue = iRecord.getProperty(firstFieldName.iterator().next());
      if (fieldValue != null) {
        if (iConsiderIn && MultiValue.isMultiValue(fieldValue)) {
          for (Object o : MultiValue.getMultiValueIterable(fieldValue, false)) {
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
