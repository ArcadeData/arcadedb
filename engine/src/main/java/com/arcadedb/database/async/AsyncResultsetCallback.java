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
package com.arcadedb.database.async;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Callback interface for asynchronous commands. If the command returns an exception, the {@link #onError(Exception)} method is invoked. Otherwise
 * {@link #onComplete(ResultSet)} is called.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface AsyncResultsetCallback {
  /**
   * Invoked as soon as the command has been executed.
   * <p>
   * <b>Ownership contract:</b> the supplied {@link ResultSet} is owned by the async executor
   * and is closed by the framework <i>immediately after this method returns</i>. Implementations
   * MUST consume (iterate / drain / materialize via {@code stream().toList()}, etc.) the result
   * set synchronously inside this callback. The reference must not be stored on a field or
   * handed off to another thread for deferred iteration.
   * <p>
   * <b>Failure mode if violated:</b> any post-close access to the underlying execution-plan
   * iterators (hasNext / next) will observe drained/closed state and may either return false
   * silently, throw {@code IllegalStateException}, or surface as a downstream
   * {@code NullPointerException} depending on the engine path. There is no defensive open-state
   * check on the {@code ResultSet} interface, so misuse is not explicit - treat this Javadoc as
   * the contract.
   * <p>
   * The framework-level close was added in the #4197 audit to avoid leaking the execution plan
   * (and the parallel-scan worker threads it pins) when the callback is fire-and-forget, such as
   * a logging-only completion handler.
   *
   * @param resultset result set to fetch, valid only for the duration of this call
   */
  void onComplete(final ResultSet resultset);

  /**
   * Invoked in case of an error in the execution or fetching of the result set.
   *
   * @param exception The exception caught
   */
  default void onError(final Exception exception) {
    // NO ACTION BY DEFAULT
  }
}
