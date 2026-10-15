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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;

import java.util.Collection;
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
 * <p>
 * A follower's refusal of leader-only work is answered with the leader named, in every mode (issue #9960), as every
 * other transport answers it: neither the refusal's message nor the placeholder says where to go, and a status code is
 * all the client has otherwise. The pipeline is also where the connection of a request is known, so this channelizer
 * publishes it for the {@code list queries} entry of the evaluation (issue #9962): TinkerPop's {@code RequestMessage}
 * carries no channel. That publication adds the channel and the port; the client itself does not depend on it, as the
 * connection's authenticated user carries one to the authorizer under any channelizer (issue #10105).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ConcealingWebSocketChannelizer extends WebSocketChannelizer {
  static final String HANDLER_NAME        = "arcade-error-concealer";
  static final String CLIENT_HANDLER_NAME = "arcade-request-client";

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
    // LAST of configure(). Relies on TinkerPop 3.8's AbstractChannelizer.initChannel adding the op selector and the op
    // executor right after configure() returns, with no executor group of their own, and on OpExecutorHandler submitting
    // the evaluation to the Gremlin pool synchronously inside its channelRead: so the submit happens on this thread, inside
    // the read below. Should a TinkerPop upgrade move it off this thread, the entry is listed without a client (the reads
    // are null-safe) and Issue9689GremlinTerminateTest, which asserts the client end to end, fails
    pipeline.addLast(CLIENT_HANDLER_NAME, RequestClientPublisher.INSTANCE);
  }

  /**
   * Publishes the connection of a request on the Netty thread for the length of its read (issue #9962): the evaluation is
   * submitted to the Gremlin pool from inside that read, where {@link GremlinPrincipalPropagatingExecutorService}
   * captures it with the user.
   */
  @ChannelHandler.Sharable
  static class RequestClientPublisher extends ChannelInboundHandlerAdapter {
    static final RequestClientPublisher INSTANCE = new RequestClientPublisher();

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      if (msg instanceof RequestMessage)
        GremlinAuthContext.publishing(RunningQuery.Client.of(ctx.channel().id().asShortText(), ctx.channel().remoteAddress()),
            () -> super.channelRead(ctx, msg));
      else
        super.channelRead(ctx, msg);
    }
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
   * {@link ArcadeDBServer#CONCEALED_ERROR_MESSAGE} and no status attributes (exception class names, stack trace). A
   * leader refusal names the leader of this server's cluster view in either mode.
   */
  static ResponseMessage conceal(final ArcadeDBServer server, final ResponseMessage response) {
    if (server == null || !isServerFailure(response.getStatus().getCode()))
      return response;
    return conceal(server.isProductionMode(), isLeaderRefusal(response) ? leaderAddress(server.getHA()) : null, response);
  }

  /**
   * Whether {@code response} answers a follower's refusal of leader-only work. The exception itself is gone by the time
   * the answer reaches the pipeline, but TinkerPop attaches the class name of every throwable along its cause chain
   * ({@code ResponseMessage.Builder.statusAttributeException}, under {@link Tokens#STATUS_ATTRIBUTE_EXCEPTIONS}, verified
   * on TinkerPop 3.8.2): should an upgrade drop that attribute, the refusal is answered as before, without the leader,
   * and {@code Issue9960GremlinLeaderRefusalTest} fails.
   */
  static boolean isLeaderRefusal(final ResponseMessage response) {
    return response.getStatus().getAttributes().get(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS) instanceof Collection<?> classes
        && classes.contains(ServerIsNotTheLeaderException.class.getName());
  }

  /** What follows the answer's text to send the client to {@code leaderAddress}. */
  private static String redirectSentence(final String leaderAddress) {
    return ". " + ServerIsNotTheLeaderException.leaderSentence(leaderAddress) + " (HTTP address; use its Gremlin Server port). Retry there";
  }

  /**
   * The HTTP address of the leader in this server's cluster view, the one a refusal names (it is read from the same
   * view when the refusal is raised), or {@code null} when there is no cluster or no leader: a refusal raised mid-election
   * names none either, and stays the ordinary retryable failure.
   */
  static String leaderAddress(final HAServerPlugin ha) {
    final String address = ha != null ? ha.getLeaderForRefusal() : null;
    return address != null && !address.isBlank() ? address : null;
  }

  /**
   * The answer for a server failure: in production mode a copy that carries {@link ArcadeDBServer#CONCEALED_ERROR_MESSAGE}
   * and no status attributes, otherwise {@code response} itself; either way followed by the leader's address when
   * {@code leaderAddress} names one (issue #9960). The Gremlin Server's port is not the HTTP one the cluster knows its
   * peers by, so the sentence says which address it is. The address is the one deliberate exception to concealment, as
   * on every other transport (HTTP, gRPC, Bolt, PostgreSQL, Redis, MongoDB): it is a cluster address, not engine detail,
   * and the only thing that makes the refusal actionable.
   */
  static ResponseMessage conceal(final boolean productionMode, final String leaderAddress, final ResponseMessage response) {
    final String redirect = leaderAddress != null ? redirectSentence(leaderAddress) : null;
    if (!productionMode) {
      if (redirect == null)
        return response;
      return ResponseMessage.build(response.getRequestId()).code(response.getStatus().getCode())
          .statusMessage(response.getStatus().getMessage() + redirect).statusAttributes(response.getStatus().getAttributes()).create();
    }
    // The client no longer sees the text, so the server log is the only copy the operator has. A failure of the query
    // (evaluation, temporary) is routine; the rest is a server fault and keeps its severity and the stack trace TinkerPop attached
    final ResponseStatusCode code = response.getStatus().getCode();
    if (code == ResponseStatusCode.SERVER_ERROR_EVALUATION || code == ResponseStatusCode.SERVER_ERROR_TEMPORARY)
      LogManager.instance().log(ConcealingWebSocketChannelizer.class, Level.INFO, "Gremlin: %s", null, response.getStatus().getMessage());
    else
      LogManager.instance().log(ConcealingWebSocketChannelizer.class, Level.WARNING, "Gremlin: %s %s", null,
          response.getStatus().getMessage(), response.getStatus().getAttributes().getOrDefault("stackTrace", ""));
    final String concealed = redirect != null ? ArcadeDBServer.CONCEALED_ERROR_MESSAGE + redirect : ArcadeDBServer.CONCEALED_ERROR_MESSAGE;
    return ResponseMessage.build(response.getRequestId()).code(code).statusMessage(concealed).create();
  }
}
