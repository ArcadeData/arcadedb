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
package com.arcadedb.server.http.handler.batch;

import java.io.IOException;

/**
 * Streaming iterator over batch records parsed from an InputStream.
 * Implementations reuse a single {@link BatchRecord} instance to minimize allocations.
 */
public interface BatchRecordStream extends AutoCloseable {

  /**
   * Returns true if there is another record available.
   */
  boolean hasNext() throws IOException;

  /**
   * Returns the current record. The returned object is reused across calls;
   * callers must extract needed data before calling {@link #hasNext()} again.
   */
  BatchRecord next();

  /**
   * Returns the current line number in the input (1-based), useful for error reporting.
   */
  int getLineNumber();

  @Override
  void close() throws IOException;
}
