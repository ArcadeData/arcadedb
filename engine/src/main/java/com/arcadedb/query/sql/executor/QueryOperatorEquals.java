/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Record;
import com.arcadedb.schema.Type;

import java.util.Arrays;

public class QueryOperatorEquals {
  public static boolean equals(Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null)
      return false;

    if (iLeft == iRight) {
      return true;
    }

    if (iLeft instanceof Result && !(iRight instanceof Result)) {
      iLeft = ((Result) iLeft).toElement();
    }

    if (iRight instanceof Result && !(iLeft instanceof Result)) {
      iRight = ((Result) iRight).toElement();
    }

    // RECORD & ORID
    if (iLeft instanceof Record)
      return comparesValues(iRight, (Record) iLeft, true);
    else if (iRight instanceof Record)
      return comparesValues(iLeft, (Record) iRight, true);
    else if (iRight instanceof Result) {
      return comparesValues(iLeft, (Record) iRight, true);
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
    return iRecord.getIdentity().equals(iValue);
  }
}
