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
package com.arcadedb.index.columnar;

/**
 * Receives the value of one property of one record. Exactly one kind of call is made per extraction, except that a read-only
 * extraction of a list may call {@link #unknownValue} for each item the dictionary does not hold yet and then {@link #ordinals} for
 * the known ones.
 */
public interface ValueSink {
  /** The dictionary ordinal of an {@link ColumnKind#ORDINAL} value. */
  void ordinal(int o);

  /** The sorted, deduplicated ordinals of an {@link ColumnKind#ORDINAL_LIST} value; only the first {@code count} entries are valid and the array is reused. */
  void ordinals(int[] sorted, int count);

  void longValue(long v);

  void doubleValue(double v);

  /** The record has no value for the property (null, absent, or not convertible). */
  void missing();

  /** Read-only extraction met a normalized value the dictionary does not hold (yet). */
  void unknownValue(Object normalized);
}
