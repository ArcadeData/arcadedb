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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

/**
 * Callback interface for asynchronous commands. If the command returns an exception, the {@link #onError(Exception)} method is invoked. Otherwise
 * {@link #onStart(ResultSet)} is called at first. The user can fetch the Resultset in this method. After the return from {@link #onStart(ResultSet)},
 * the result set is fetched and foreach result the method {@link #onNext(Result)} is invoked by passing the current Result object. When the fetching of the
 * result set is complete, the {@link #onComplete()} method is called.
 * <p>
 * It's suggested to extend the @{@link AbstractAsyncResultsetCallback} abstract class and implement {@link #onStart(ResultSet)} to fetch the result set all in
 * one call, otherwise implement {@link #onNext} method to be invoked foreach record in the result set. In case both {@link #onStart(ResultSet)} and
 * {@link #onNext(Result)} are implemented, whatever is remained left from fetching in the result set is invoked in multiple {@link #onNext(Result)} calls.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface AsyncResultsetCallback {
  /**
   * Invoked as soon as the command has been executed.
   *
   * @param resultset result set to fetch
   */
  default void onStart(ResultSet resultset) {
  }

  /**
   * Invoked per single result in the result set.
   *
   * @return true to continue fetching otherwise false. If false is returned, the fetching stops and the {@link #onComplete()} method is never invoked.
   */
  default boolean onNext(Result result) {
    return true;
  }

  /**
   * Invoked when the fetching of the entire result set has been completed.
   */
  default void onComplete() {
  }

  /**
   * Invoked in case of an error in the execution or fetching of the result set.
   *
   * @param exception The exception caught
   */
  default void onError(Exception exception) {
  }
}
