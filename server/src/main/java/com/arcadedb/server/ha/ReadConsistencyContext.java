/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;

/**
 * Thread-local context for read consistency. Set by the HTTP handler before query execution
 * and read by the HA replicated database to enforce the requested consistency level.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ReadConsistencyContext {

  private static final ThreadLocal<ReadConsistencyContext> CONTEXT = new ThreadLocal<>();

  public final Database.READ_CONSISTENCY consistency;
  public final long                      readAfterIndex;

  private ReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    this.consistency = consistency;
    this.readAfterIndex = readAfterIndex;
  }

  public static void set(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  public static ReadConsistencyContext get() {
    return CONTEXT.get();
  }

  public static void clear() {
    CONTEXT.remove();
  }
}
