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
package com.arcadedb.index;

import com.arcadedb.database.Cursor;
import com.arcadedb.database.Identifiable;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Cursor to browse an result set from an index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface IndexCursor extends Cursor {
  Object[] getKeys();

  Identifiable getRecord();

  /**
   * Returns the score of the current entry. The score is used by full-text indexes to indicate the relevance of the result.
   * For indexes that don't support scoring (e.g., regular LSM indexes), this method returns 0.
   *
   * @return the score of the current entry, or 0 if scoring is not supported
   */
  default int getScore() {
    return 0;
  }

  default void close() {
    // NO ACTIONS
  }

  default String dumpStats() {
    return "no-stats";
  }

  BinaryComparator getComparator();

  byte[] getBinaryKeyTypes();
}
