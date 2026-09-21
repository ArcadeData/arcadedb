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

import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;


/**
 * Base handler for database-scoped endpoints that receive binary (non-JSON) request bodies.
 * Captures raw bytes from the request instead of interpreting them as a string.
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681. Both of its subclasses - the Prometheus
 * {@code remote_write} and {@code remote_read} endpoints under {@code /api/v1/ts/{database}/prom} - are
 * database-scoped and had to start honouring {@code arcadedb-session-id}, and Java has one superclass to
 * spend. The command that says these two are the whole population:
 * <pre>
 * $ grep -rn "extends AbstractBinaryHttpHandler" --include="*.java" .
 * ./server/src/main/java/com/arcadedb/server/http/handler/PostPrometheusReadHandler.java
 * ./server/src/main/java/com/arcadedb/server/http/handler/PostPrometheusWriteHandler.java
 * </pre>
 * A future binary endpoint that is NOT database-scoped therefore cannot use this class as-is; it would want
 * the body capture below over {@code AbstractServerHttpHandler} instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractBinaryHttpHandler extends DatabaseAbstractHandler {
  /**
   * This request's body, attached to the exchange rather than kept in a field.
   * <p>
   * It used to be an instance field, and a handler is a SINGLETON - one instance is registered on the route and
   * serves every request. {@link #parseRequestPayload} and {@code execute} are two separate calls from
   * {@code AbstractServerHttpHandler.handleRequest}, with authentication, the idempotency reservation and the
   * session resolution in between, so two concurrent requests interleaved as: T1 parses body1, T2 overwrites
   * the field with body2, T1 executes against T2's bytes, T2 executes against them again. On
   * {@code POST /api/v1/ts/{database}/prom/write} that silently ingests one remote-write payload twice and
   * loses the other, and Prometheus remote-write is a path whose normal deployment is several concurrent
   * shippers. This is the same defect issue #7683 reported for {@code PostTimeSeriesWriteHandler}'s string
   * body, in the class that holds the byte one; moving these handlers onto {@link DatabaseAbstractHandler}
   * widens the window by adding the session resolution to what runs between the two calls, so it is fixed in
   * the same change rather than left wider than it was found.
   */
  private static final AttachmentKey<byte[]> RAW_BINARY_PAYLOAD = AttachmentKey.create(byte[].class);

  public AbstractBinaryHttpHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected boolean requiresJsonPayload() {
    return false;
  }

  /**
   * The bytes THIS request arrived with, or {@code null} when the body was empty or could not be read.
   */
  protected static byte[] rawBytes(final HttpServerExchange exchange) {
    return exchange.getAttachment(RAW_BINARY_PAYLOAD);
  }

  /**
   * The body these routes bind their idempotency key to (issue #7704).
   * <p>
   * {@link #parseRequestPayload} answers {@code null} for the STRING payload by construction, and the key folded
   * in only that: so two {@code remote_write} requests carrying different samples under one {@code X-Request-Id}
   * hashed to the same key, the second was a cache hit replayed the first's {@code 204}, and its samples were
   * never appended. Handing the pipeline the bytes restores on these routes the very protection the body was
   * added to the key to provide.
   * <p>
   * Read from the exchange, not from a field, for the reason the attachment exists at all: a handler is a
   * singleton, and this is called between {@code parseRequestPayload} and {@code execute} on a request another
   * thread may be interleaved with.
   */
  @Override
  protected byte[] idempotencyBodyBytes(final HttpServerExchange exchange) {
    return rawBytes(exchange);
  }

  @Override
  protected String parseRequestPayload(final HttpServerExchange e) {
    if (!e.isInIoThread() && !e.isBlocking())
      e.startBlocking();

    // The shared bounded reader, so arcadedb.server.httpBodyContentMaxSize bounds this route's body too - it
    // used to be read through Receiver.receiveFullBytes, which enforces no cap on a body that declares no
    // length (issue #7772).
    final byte[] body = readRequestBody(e);
    if (body != null)
      e.putAttachment(RAW_BINARY_PAYLOAD, body);
    return null; // no string payload needed
  }
}
