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
package com.arcadedb.server.gremlin;

import com.arcadedb.server.ServerException;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * The {@code session} processor of the Gremlin Server, served to the server administrator only.
 * <p>
 * A session runs its requests on a thread of its own, created by TinkerPop, and not on the Gremlin pool of this plugin,
 * the one {@link GremlinPrincipalPropagatingExecutorService} binds the authenticated user on before it runs a request.
 * The type access of a group is applied to the user bound on the thread, so sessions are served to the user that no group
 * limits, the server administrator, and refused here to every other user, before TinkerPop creates the session. This is
 * the single point every request of a session goes through whatever its operation is, and it does not depend on the
 * channelizer, as the processor is selected by name from TinkerPop's processor table.
 * <p>
 * TinkerPop fills that table from the {@code ServiceLoader} when {@link OpLoader} is first used, and offers no way to
 * change it, so the entry is replaced through its field. The plugin does not start when that is not possible.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class SessionRestrictingOpProcessor implements OpProcessor {
  static final String REFUSAL = "Gremlin sessions are reserved to the server administrator";

  private final OpProcessor delegate;

  private SessionRestrictingOpProcessor(final OpProcessor delegate) {
    this.delegate = delegate;
  }

  /**
   * Replaces the session processor of the processor table with this restriction, once: a processor already restricted is
   * left alone, so a second Gremlin Server of the same JVM does not wrap it twice. To be called before the Gremlin Server
   * is built, which initializes the processors of the table.
   */
  static void install() {
    final Map<String, OpProcessor> processors = processorTable();
    synchronized (OpLoader.class) {
      final OpProcessor session = processors.get(SessionOpProcessor.OP_PROCESSOR_NAME);
      if (session != null && !(session instanceof SessionRestrictingOpProcessor))
        processors.put(SessionOpProcessor.OP_PROCESSOR_NAME, new SessionRestrictingOpProcessor(session));
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, OpProcessor> processorTable() {
    try {
      final Field field = OpLoader.class.getDeclaredField("processors");
      field.setAccessible(true);
      return (Map<String, OpProcessor>) field.get(null);
    } catch (final ReflectiveOperationException | RuntimeException e) {
      throw new ServerException("Cannot restrict the sessions of the Gremlin Server: " + e, e);
    }
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public void init(final Settings settings) {
    delegate.init(settings);
  }

  @Override
  public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
    final AuthenticatedUser user = ctx.getChannelHandlerContext().channel().attr(StateKey.AUTHENTICATED_USER).get();
    if (user == null || !ServerSecurityUser.isServerAdministrator(user.getName()))
      throw new OpProcessorException(REFUSAL,
          ResponseMessage.build(ctx.getRequestMessage()).code(ResponseStatusCode.UNAUTHORIZED).statusMessage(REFUSAL).create());
    return delegate.select(ctx);
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
