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

import java.io.IOException;

public class DatabaseIsClosedException extends ArcadeDBException {
  private final String databaseName;

  public DatabaseIsClosedException(final String s) {
    super(s);
    this.databaseName = null;
  }

  public DatabaseIsClosedException(final String s, final IOException e) {
    super(s, e);
    this.databaseName = null;
  }

  /**
   * The handle of {@code databaseName} was closed: the message is the name, as it always was, and the name is kept apart so a
   * caller can ask what became of the database (issue #10872).
   */
  protected DatabaseIsClosedException(final String s, final String databaseName) {
    super(s);
    this.databaseName = databaseName;
  }

  /** The handle of {@code databaseName} was closed (issue #10872). */
  public static DatabaseIsClosedException of(final String databaseName) {
    return new DatabaseIsClosedException(databaseName, databaseName);
  }

  /** The name of the database whose handle was closed, or {@code null} when the thrower did not say. */
  public String getDatabaseName() {
    return databaseName;
  }
}
