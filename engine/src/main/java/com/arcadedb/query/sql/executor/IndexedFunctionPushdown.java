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

/**
 * What the plan hands an indexed function along with its condition, so that it can do less than answer every match in
 * its own order (issues #9895 and #9909).
 *
 * @param limit     how many rows of the answer the plan can consume, or -1 for all of them. With {@link Order#FUNCTION}
 *                  they are the first rows of the function's own best-first order, with a RID order the first rows of
 *                  that order
 * @param ridFilter the {@code @rid} conditions of the WHERE block the answer must satisfy, which the plan no longer
 *                  checks after the fetch, or null for none
 * @param order     the order the plan needs the rows in: the function's own, or RID order when the function
 *                  {@link IndexableSQLFunction#supportsRIDOrder() supports it} and the statement orders by {@code @rid}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record IndexedFunctionPushdown(int limit, RIDFilter ridFilter, Order order) {
  public static final IndexedFunctionPushdown NONE = new IndexedFunctionPushdown(-1, null, Order.FUNCTION);

  public enum Order {
    /** The function's own order: best first for a scoring function. */
    FUNCTION,
    RID_ASC,
    RID_DESC
  }

  public static IndexedFunctionPushdown ofLimit(final int limit) {
    return limit < 0 ? NONE : new IndexedFunctionPushdown(limit, null, Order.FUNCTION);
  }
}
