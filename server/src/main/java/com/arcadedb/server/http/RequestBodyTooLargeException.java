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
 * Thrown when a request body that declared a {@code Content-Encoding} expands past
 * {@code arcadedb.server.httpBodyContentDecompressedMaxSize} (issue #8084). Mapped to HTTP 413 by the request
 * handler, the same way {@link ResultSetTooLargeException} is on the way out.
 * <p>
 * The cap on the wire bounds only what arrives. On a route that decodes the body - the InfluxDB line-protocol
 * ingest reads gzip, the Prometheus remote_write and remote_read endpoints read Snappy - the decoded result is
 * materialized whole in heap, so without a second bound the wire cap is a compression-ratio multiplier and not a
 * limit at all: line protocol is close to the best case for DEFLATE, and an accepted 100MB body is worth tens of
 * GB.
 * <p>
 * Raised BEFORE the decoded bytes exist wherever the format allows it - Snappy declares its uncompressed length in
 * the payload, so the allocation never happens - and otherwise from a counting stream that stops reading the moment
 * the budget is spent, so a refusal costs the budget and never the body.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RequestBodyTooLargeException extends ServerException {
  private final long maxSize;

  public RequestBodyTooLargeException(final String message, final long maxSize) {
    super(message);
    this.maxSize = maxSize;
  }

  /**
   * The ceiling that refused the body. Travels to the client in the error body's {@code exceptionArgs} field,
   * which - unlike the free-form {@code detail} - is emitted in production mode too, so a caller always learns the
   * number it has to stay under.
   */
  public long getMaxSize() {
    return maxSize;
  }
}
