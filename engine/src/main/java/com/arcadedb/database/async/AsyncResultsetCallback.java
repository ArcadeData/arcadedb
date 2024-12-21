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
   *
   * @param resultset result set to fetch
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
