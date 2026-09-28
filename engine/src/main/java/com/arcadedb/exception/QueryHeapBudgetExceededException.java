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
package com.arcadedb.exception;

/**
 * A query was refused heap for its in-memory buffers because the queries running at the same time already hold most
 * of {@link com.arcadedb.GlobalConfiguration#QUERY_MAX_HEAP_RAM} (issue #8591).
 * <p>
 * Transient by construction: the query fits the budget on its own, so the same request re-issued once the others
 * release what they hold can succeed. That is why it is classified as retryable on every wire (503 on HTTP), and why it
 * is NOT a {@link NeedRetryException}: it is no transaction conflict, and a commit loop that retries those on the spot
 * would only run into the same full budget again. A query that alone needs more than the whole budget fails with a
 * plain {@link CommandExecutionException} instead, since no retry can ever help it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryHeapBudgetExceededException extends CommandExecutionException {
  public QueryHeapBudgetExceededException(final String message) {
    super(message);
  }
}
