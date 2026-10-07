/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.server.gremlin;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;

import java.util.logging.Level;

/**
 * The Gremlin Server's WebSocket channelizer with one addition: in production mode, an answer that carries a server-side
 * failure leaves with {@link ArcadeDBServer#CONCEALED_ERROR_MESSAGE} instead of the exception's own text (issue #9317).
 * <p>
 * TinkerPop puts {@code Throwable.getMessage()} on the wire for an evaluation failure, and attaches the exception class
 * names and the stack trace as status attributes. For a duplicated key that text holds the customer's stored values,
 * which HTTP, gRPC, PostgreSQL and MongoDB already conceal. The text is written in many places inside TinkerPop, so the
 * one reliable point is the pipeline, right before the response is serialised.
 * <p>
 * Only the codes that stand for a failure of the server or of the engine are rewritten. Authentication and
 * authorization answers, malformed or invalid requests, a timeout and the {@code fail()} step carry bounded text or the
 * client's own text and keep it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConcealingWebSocketChannelizer extends WebSocketChannelizer {
  static final String HANDLER_NAME = "arcade-error-concealer";

  private ArcadeDBServer server;

  @Override
  public void init(final ServerGremlinExecutor serverGremlinExecutor) {
    super.init(serverGremlinExecutor);
    // The plugin hands the server to the authorizer, the one piece of configuration TinkerPop passes through untouched
    if (settings.authorization != null && settings.authorization.config != null
        && settings.authorization.config.get("server") instanceof ArcadeDBServer arcadeDBServer)
      server = arcadeDBServer;
    else
      // Without the server the production mode cannot be read, so nothing would be concealed: say so instead of leaking silently
      LogManager.instance().log(this, Level.WARNING,
          "Gremlin Server error concealment is inactive: the ArcadeDB server was not found in the authorization configuration");
  }

  @Override
  public void configure(final ChannelPipeline pipeline) {
    super.configure(pipeline);
    // Positioned after the response encoder: an outbound write travels towards the head, so everything written by the
    // handlers that follow in the pipeline reaches this one before it is serialised
    pipeline.addAfter("response-frame-encoder", HANDLER_NAME, new ErrorConcealer(server));
  }

  /** True for the codes whose status message is the text of an exception the engine or the server raised. */
  static boolean isServerFailure(final ResponseStatusCode code) {
    return switch (code) {
      case SERVER_ERROR, SERVER_ERROR_TEMPORARY, SERVER_ERROR_EVALUATION, SERVER_ERROR_SERIALIZATION -> true;
      default -> false;
    };
  }

  @ChannelHandler.Sharable
  static class ErrorConcealer extends ChannelOutboundHandlerAdapter {
    private final ArcadeDBServer server;

    ErrorConcealer(final ArcadeDBServer server) {
      this.server = server;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
      super.write(ctx, msg instanceof ResponseMessage response ? conceal(server, response) : msg, promise);
    }
  }

  /**
   * The answer to put on the wire: {@code response} itself, or in production mode a copy of a server failure that carries
   * {@link ArcadeDBServer#CONCEALED_ERROR_MESSAGE} and no status attributes (exception class names, stack trace).
   */
  static ResponseMessage conceal(final ArcadeDBServer server, final ResponseMessage response) {
    if (server == null || !isServerFailure(response.getStatus().getCode()) || !server.isProductionMode())
      return response;
    // The client no longer sees the text, so the server log is the only copy the operator has. A failure of the query
    // (evaluation, temporary) is routine; the rest is a server fault and keeps its severity and the stack trace TinkerPop attached
    final ResponseStatusCode code = response.getStatus().getCode();
    if (code == ResponseStatusCode.SERVER_ERROR_EVALUATION || code == ResponseStatusCode.SERVER_ERROR_TEMPORARY)
      LogManager.instance().log(ConcealingWebSocketChannelizer.class, Level.INFO, "Gremlin: %s", null, response.getStatus().getMessage());
    else
      LogManager.instance().log(ConcealingWebSocketChannelizer.class, Level.WARNING, "Gremlin: %s %s", null,
          response.getStatus().getMessage(), response.getStatus().getAttributes().getOrDefault("stackTrace", ""));
    return ResponseMessage.build(response.getRequestId()).code(response.getStatus().getCode())
        .statusMessage(ArcadeDBServer.CONCEALED_ERROR_MESSAGE).create();
  }
}
