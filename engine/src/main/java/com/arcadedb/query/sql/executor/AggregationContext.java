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

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Created by luigidellaquila on 16/07/16.
 */
@ExcludeFromJacocoGeneratedReport
public interface AggregationContext {
  Object getFinalValue();

  void apply(Result next, CommandContext context);

  /**
   * Whether {@link #merge} can fold into this context another one of the same aggregation fed a disjoint part of the
   * rows - the partial aggregation a parallel scan runs in its workers (issue #8523).
   */
  default boolean canMerge() {
    return false;
  }

  /** Folds {@code other}'s state into this one. Only called when {@link #canMerge()} is {@code true}. */
  default void merge(final AggregationContext other) {
    throw new UnsupportedOperationException("This aggregation cannot merge partial results");
  }
}
