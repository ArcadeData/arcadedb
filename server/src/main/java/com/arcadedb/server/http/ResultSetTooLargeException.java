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
package com.arcadedb.server.http;

import com.arcadedb.server.ServerException;

/**
 * Thrown when a single HTTP response would carry more rows than
 * {@code arcadedb.server.httpQueryMaxResultRows} allows (issue #5719). Mapped to HTTP 413 by the request
 * handler.
 * <p>
 * The ceiling refuses instead of truncating on purpose: a response silently cut short is indistinguishable
 * from a complete one, which is the defect issue #5711 fixed, and re-introducing it for the callers that
 * state a limit of their own would only move it. Raising it from inside the handler also rolls the
 * auto-commit transaction back, so a write command whose result nobody will ever see is not committed
 * behind an error status.
 */
public class ResultSetTooLargeException extends ServerException {
  private final int maxResultRows;

  public ResultSetTooLargeException(final String message, final int maxResultRows) {
    super(message);
    this.maxResultRows = maxResultRows;
  }

  /**
   * The ceiling that refused the response. Travels to the client in the error body's {@code exceptionArgs}
   * field, which - unlike the free-form {@code detail} - is emitted in production mode too, so a caller always
   * learns the number it has to stay under.
   */
  public int getMaxResultRows() {
    return maxResultRows;
  }
}
