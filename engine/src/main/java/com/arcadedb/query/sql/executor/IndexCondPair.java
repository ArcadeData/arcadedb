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

import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BinaryCondition;

import java.util.Objects;

/**
 * For internal use.
 * It is used to keep info about an index range search,
 * where the main condition has the lower bound and the additional condition has the upper bound on last field only
 */
class IndexCondPair {

  final AndBlock        mainCondition;
  final BinaryCondition additionalRange;

  public IndexCondPair(AndBlock keyCondition, BinaryCondition additionalRangeCondition) {
    this.mainCondition = keyCondition;
    this.additionalRange = additionalRangeCondition;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    IndexCondPair that = (IndexCondPair) o;

    if (!Objects.equals(mainCondition, that.mainCondition))
      return false;
    return Objects.equals(additionalRange, that.additionalRange);
  }

  @Override public int hashCode() {
    int result = mainCondition != null ? mainCondition.hashCode() : 0;
    result = 31 * result + (additionalRange != null ? additionalRange.hashCode() : 0);
    return result;
  }
}
