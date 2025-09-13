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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;

public class LeOperator extends SimpleNode implements BinaryCompareOperator {
  public LeOperator(final int id) {
    super(id);
  }

  @Override
  public boolean execute(final DatabaseInternal database, Object left, Object right) {
    if (left == right)
      return true;

    if (left == null || right == null)
      return false;

    if (!left.getClass().equals(right.getClass())) {
      if (left instanceof Number number && right instanceof Number number1) {
        final Number[] couple = Type.castComparableNumber(number, number1);
        left = couple[0];
        right = couple[1];
      } else {
        right = Type.convert(database, right, left.getClass());
      }
    }

    if (right == null)
      return false;
    return BinaryComparator.compareTo(left, right) <= 0;
  }

  @Override
  public String toString() {
    return "<=";
  }

  @Override
  public LeOperator copy() {
    return this;
  }

  @Override
  public boolean isRangeOperator() {
    return true;
  }

  @Override
  public boolean equals(final Object obj) {
    return obj != null && obj.getClass().equals(this.getClass());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public boolean isInclude() {
    return true;
  }

  @Override
  public boolean isLess() {
    return true;
  }

  @Override
  public boolean isGreater() {
    return false;
  }
}
/* JavaCC - OriginalChecksum=8b3232c970fd654af947274a5f384a93 (do not edit this line) */
