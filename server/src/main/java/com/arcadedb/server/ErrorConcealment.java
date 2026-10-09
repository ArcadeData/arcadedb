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
package com.arcadedb.server;

import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * The text a client is told for a failure the ENGINE raised, on the wire surfaces that word their own error frames: the
 * message itself, or, in production mode ({@link ArcadeDBServer#isProductionMode()}), the one placeholder every surface
 * uses, {@link ArcadeDBServer#CONCEALED_ERROR_MESSAGE}. The engine text of a duplicated key carries the customer's stored
 * key values, and an internal failure can carry file paths or URLs (issue #8749, after #7472, #7760 and #8931).
 * <p>
 * The structured part a client branches on - an error code, a Neo4j status, a RESP prefix, an exception class name -
 * is not text and stays. So does text the protocol layer writes itself about the request (a missing property, an
 * unknown action, a refused credential): it is bounded, carries no stored data, and is what the client needs to fix
 * the request. Only the engine's free text goes through here.
 * <p>
 * Concealing and logging are ONE decision: the placeholder tells the operator to check the server log, so a caller
 * that concealed without writing that entry would destroy the detail rather than move it. A failure the classifier
 * cannot attribute to the client is a server fault and is logged at {@code SEVERE} with its stack trace; anything else
 * (a duplicated key, a syntax error, a validation refusal) is routine and logged with its message alone, at the level
 * the caller picks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ErrorConcealment {
  private ErrorConcealment() {
  }

  /** Whether {@code server} conceals engine text: false with no server, as a surface running outside one is not in production. */
  public static boolean isConcealing(final ArcadeDBServer server) {
    return server != null && server.isProductionMode();
  }

  /**
   * {@link #clientMessage(boolean, Object, String, String, Throwable, Level)} logging a client-caused failure at
   * {@code INFO}, as the PostgreSQL wire protocol does.
   */
  public static String clientMessage(final boolean conceal, final Object requester, final String surface, final String message,
      final Throwable cause) {
    return clientMessage(conceal, requester, surface, message, cause, Level.INFO);
  }

  /**
   * The text the client is told for {@code cause}.
   *
   * @param conceal           whether the server runs in production mode
   * @param requester         the logging context
   * @param surface           the wire surface, prefixed to the log entry ("Bolt", "Redis", ...)
   * @param message           the text the client gets when nothing is concealed
   * @param cause             the failure, which picks the log level
   * @param clientCausedLevel the level a failure the client caused is logged at: {@code FINE} on a path that reports one
   *                          entry per row of a bulk load, where conflicts are routine and plentiful
   */
  public static String clientMessage(final boolean conceal, final Object requester, final String surface, final String message,
      final Throwable cause, final Level clientCausedLevel) {
    if (!conceal)
      return message;
    // THE TEXT CAN CARRY CLIENT DATA (A DUPLICATED KEY'S VALUES): ONE LINE, SO IT CANNOT FORGE ENTRIES IN A LINE-ORIENTED LOG
    final String logged = singleLine(message);
    if (ErrorCategory.of(cause) == ErrorCategory.SERVER)
      LogManager.instance().log(requester, Level.SEVERE, "%s: %s (concealed from the client in production mode)", cause, surface,
          logged);
    else
      LogManager.instance().log(requester, clientCausedLevel, "%s: %s (concealed from the client in production mode)", null, surface,
          logged);
    return ArcadeDBServer.CONCEALED_ERROR_MESSAGE;
  }

  /** {@code text} with its line breaks replaced by spaces; the same instance when it has none. */
  static String singleLine(final String text) {
    if (text == null || (text.indexOf('\n') < 0 && text.indexOf('\r') < 0))
      return text;
    return text.replace("\r\n", " ").replace('\r', ' ').replace('\n', ' ');
  }

  /**
   * The text the client is told for a failure the caller has ALREADY logged with its detail, so nothing is logged twice:
   * the message, or the placeholder in production mode.
   */
  public static String loggedClientMessage(final boolean conceal, final String message) {
    return conceal ? ArcadeDBServer.CONCEALED_ERROR_MESSAGE : message;
  }
}
