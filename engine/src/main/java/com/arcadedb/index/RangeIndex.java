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

/**
 * Basic Range Index interface. Supports range queries and iterations.
 */
public interface RangeIndex extends Index {
  /**
   * The returning iterator does not skip deleted entries and it might contains duplicated entries.
   * WARNING: this method does not read pending changes in transaction.
   */
  IndexCursor iterator(boolean ascendingOrder);

  /**
   * The returning iterator does not skip deleted entries and it might contains duplicated entries.
   * WARNING: this method does not read pending changes in transaction.
   */
  IndexCursor iterator(boolean ascendingOrder, Object[] fromKeys, boolean inclusive);

  /**
   * The returning iterator does not skip deleted entries and it might contains duplicated entries.
   * WARNING: this method does not read pending changes in transaction.
   */
  IndexCursor range(boolean ascending, Object[] beginKeys, boolean beginKeysInclusive, Object[] endKeys, boolean endKeysInclusive);
}
