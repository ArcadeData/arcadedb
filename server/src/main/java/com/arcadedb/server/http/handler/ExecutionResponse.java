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
package com.arcadedb.server.http.handler;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.nio.ByteBuffer;

public class ExecutionResponse {
  private final int    code;
  private final String response;
  private final byte[] binary;
  /**
   * Response headers to send with this response, as name/value pairs, or null when there are none - which is what
   * almost every response has, so the common case allocates nothing. Used to carry the headers a follower relays from
   * the leader's answer to a forwarded request, such as {@code Retry-After} (issue #8343).
   */
  private String[]     headers;
  /**
   * The answer to replay, from the idempotency cache, to a retry of this request that asks for
   * {@code text/event-stream}; null when there is none (issue #8331).
   */
  private String       eventStreamReplay;
  /**
   * True when the handler has already written the response itself, as a stream, and this object only describes it
   * for the idempotency cache: {@link #send} must not write it again (issue #8331).
   */
  private boolean      alreadySent;

  public ExecutionResponse(final int code, final String response) {
    this.code = code;
    this.response = response;
    this.binary = null;
  }

  public ExecutionResponse(final int code, final byte[] bytes) {
    this.code = code;
    this.response = null;
    this.binary = bytes;
  }

  public int getCode() {
    return code;
  }

  public String getResponse() {
    return response;
  }

  public byte[] getBinary() {
    return binary;
  }

  public boolean isBinary() {
    return binary != null;
  }

  /**
   * Adds a header to send with this response. A second call with the same name replaces the first value.
   *
   * @return this response, for chaining
   */
  public ExecutionResponse setHeader(final String name, final String value) {
    if (headers != null)
      for (int i = 0; i < headers.length; i += 2)
        if (headers[i].equalsIgnoreCase(name)) {
          headers[i + 1] = value;
          return this;
        }

    final int size = headers == null ? 0 : headers.length;
    final String[] extended = new String[size + 2];
    if (headers != null)
      System.arraycopy(headers, 0, extended, 0, size);
    extended[size] = name;
    extended[size + 1] = value;
    headers = extended;
    return this;
  }

  /**
   * Sets the complete SSE body a retry asking for {@code text/event-stream} is replayed from the idempotency cache.
   *
   * @return this response, for chaining
   */
  public ExecutionResponse setEventStreamReplay(final String eventStreamReplay) {
    this.eventStreamReplay = eventStreamReplay;
    return this;
  }

  public String getEventStreamReplay() {
    return eventStreamReplay;
  }

  /**
   * Marks this response as already written by the handler, as a stream: it is then only what the idempotency cache
   * records of the request, and {@link #send} writes nothing.
   *
   * @return this response, for chaining
   */
  public ExecutionResponse markAlreadySent() {
    this.alreadySent = true;
    return this;
  }

  public boolean isAlreadySent() {
    return alreadySent;
  }

  /** The value of a header set with {@link #setHeader}, matched case-insensitively, or null. */
  public String getHeader(final String name) {
    if (headers != null)
      for (int i = 0; i < headers.length; i += 2)
        if (headers[i].equalsIgnoreCase(name))
          return headers[i + 1];
    return null;
  }

  /** Copies the headers set with {@link #setHeader} onto {@code target}. Package-private for tests. */
  void applyHeaders(final HeaderMap target) {
    if (headers != null)
      for (int i = 0; i < headers.length; i += 2)
        target.put(HttpString.tryFromString(headers[i]), headers[i + 1]);
  }

  public void send(final HttpServerExchange exchange) {
    if (alreadySent)
      return;
    exchange.setStatusCode(code);
    applyHeaders(exchange.getResponseHeaders());
    if (binary != null) {
      exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, binary.length);
      exchange.getResponseSender().send(ByteBuffer.wrap(binary));
    } else
      exchange.getResponseSender().send(response);
  }
}
