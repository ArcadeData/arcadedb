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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Base handler for endpoints that receive binary (non-JSON) request bodies.
 * Captures raw bytes from the request instead of interpreting them as a string.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractBinaryHttpHandler extends AbstractServerHttpHandler {

  protected byte[] rawBytes;

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

  @Override
  protected String parseRequestPayload(final HttpServerExchange e) {
    if (!e.isInIoThread() && !e.isBlocking())
      e.startBlocking();

    final AtomicReference<byte[]> result = new AtomicReference<>();
    e.getRequestReceiver().receiveFullBytes(
        (exchange, data) -> result.set(data),
        (exchange, err) -> {
          LogManager.instance().log(this, Level.SEVERE, "receiveFullBytes completed with an error: %s", err, err.getMessage());
          exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
          exchange.getResponseSender().send("Invalid Request");
        });
    rawBytes = result.get();
    return null; // no string payload needed
  }
}
