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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Created by luigidellaquila on 12/11/14.
 */
@ExcludeFromJacocoGeneratedReport
public interface BinaryCompareOperator {
  boolean execute(DatabaseInternal database, Object left, Object right);

  BinaryCompareOperator copy();

  default boolean isRangeOperator() {
    return false;
  }

  boolean isLess();

  boolean isGreater();

  boolean isInclude();

  default boolean isGreaterInclude() {
    return isGreater() && isInclude();
  }

  default boolean isLessInclude() {
    return isLess() && isInclude();
  }
}
