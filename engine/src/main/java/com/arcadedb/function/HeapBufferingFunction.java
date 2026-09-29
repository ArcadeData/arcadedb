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
package com.arcadedb.function;

import com.arcadedb.query.sql.executor.OperationHeapLimit;

/**
 * An aggregate function that holds the values it aggregates in heap until the aggregation ends: collect(), a DISTINCT
 * aggregate, percentileCont() (issue #8591).
 * <p>
 * The step that runs the aggregation hands the function the {@link OperationHeapLimit} of its own buffer, so what the
 * function holds is charged to that operation and released with it, when the step's groups go. A function nobody hands
 * one charges an operation of its own, which it gives back when it hands out its result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface HeapBufferingFunction {
  /** Charges what the function holds to {@code owner}, from now on. */
  void setHeapLimit(OperationHeapLimit owner);

  /** Hands {@code owner} to {@code function} when the function buffers what it aggregates, and returns the function. */
  static <T> T adopt(final T function, final OperationHeapLimit owner) {
    if (owner != null && function instanceof HeapBufferingFunction buffering)
      buffering.setHeapLimit(owner);
    return function;
  }
}
