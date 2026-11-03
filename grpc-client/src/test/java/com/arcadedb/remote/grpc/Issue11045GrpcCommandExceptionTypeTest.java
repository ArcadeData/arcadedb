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
package com.arcadedb.remote.grpc;

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ExceptionPackageCensus;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteExceptionRebuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11045, the gRPC side: the client rebuilt the server's exception from its own table of names, with the command family
 * absent, and fell back to the status code, which reads none of it. No server is required.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11045GrpcCommandExceptionTypeTest {
  /** The few that cannot come back as themselves, with what they come back as. */
  private static final Map<String, Class<?>> REBUILT_AS = Map.of(
      // its (String) constructor takes a type name: rebuilt as the AlreadyExistsException it extends (issue #10153)
      TypeAlreadyExistsException.class.getName(), AlreadyExistsException.class,
      // its constructor takes the names of the missing parameters: rebuilt as the semantic error it extends
      CommandParameterMissingException.class.getName(), CommandSemanticException.class);


  @Test
  void theCommandFamilyKeepsItsType() {
    assertThat(GrpcClientErrorMapper.toException(withClass(Status.INVALID_ARGUMENT.withDescription("SQL syntax error"),
        CommandSQLParsingException.class.getName()))).isExactlyInstanceOf(CommandSQLParsingException.class)
        .hasMessage("SQL syntax error");
    assertThat(GrpcClientErrorMapper.toException(withClass(Status.INTERNAL.withDescription("Unknown function name 'nosuchfn'"),
        CommandExecutionException.class.getName()))).isExactlyInstanceOf(CommandExecutionException.class);
    assertThat(GrpcClientErrorMapper.toException(withClass(Status.FAILED_PRECONDITION.withDescription("Transaction not begun"),
        TransactionException.class.getName()))).isExactlyInstanceOf(TransactionException.class);
  }

  /** An UNAVAILABLE keeps the retry contract: a rebuilt type has to be retryable, or the status mapping answers. */
  @Test
  void unavailableStaysRetryable() {
    assertThat(GrpcClientErrorMapper.toException(withClass(Status.UNAVAILABLE.withDescription("Timeout on locking"),
        LockTimeoutException.class.getName()))).isExactlyInstanceOf(LockTimeoutException.class);
    assertThat(GrpcClientErrorMapper.toException(withClass(Status.UNAVAILABLE.withDescription("Transient"),
        CommandExecutionException.class.getName()))).isExactlyInstanceOf(NeedRetryException.class);
  }

  /** A name this client has no class for still comes back as a RemoteException, which is an ArcadeDBException now. */
  @Test
  void anUnknownNameIsStillAnArcadeDBException() {
    assertThat(GrpcClientErrorMapper.toException(withClass(Status.INTERNAL.withDescription("The plugin refused"),
        "com.arcadedb.plugin.SomePluginException"))).isExactlyInstanceOf(RemoteException.class)
        .isInstanceOf(ArcadeDBException.class);
  }

  /**
   * The guarantee: every concrete exception of the engine's exception package comes back as itself, so the mapping cannot
   * drift again the next time the engine gains one. The few that cannot are listed with what they come back as.
   */
  @Test
  void everyExceptionOfTheEnginePackageComesBackAsItself() throws Exception {
    final List<String> wrong = new ArrayList<>();
    for (final Class<?> type : ExceptionPackageCensus.concreteExceptions()) {
      final RuntimeException result = GrpcClientErrorMapper.toException(
          withClass(Status.INTERNAL.withDescription("boom"), type.getName()));
      final Class<?> expected = REBUILT_AS.getOrDefault(type.getName(), type);
      if (result.getClass() != expected)
        wrong.add(type.getName() + " came back as " + result.getClass().getName());
    }
    assertThat(wrong).isEmpty();
  }

  /**
   * Issue #11198: the census above reads a package, the authority's table draws from several. This one reads the table's own key
   * set, so an entry cannot leave {@link RemoteExceptionRebuilder} without a test noticing, wherever its class lives.
   */
  @Test
  void everyNameOfTheAuthorityComesBackAsItself() throws Exception {
    final List<String> wrong = new ArrayList<>();
    for (final String name : RemoteExceptionRebuilder.names()) {
      final RuntimeException result = GrpcClientErrorMapper.toException(withClass(Status.INTERNAL.withDescription("boom"), name));
      final Class<?> expected = REBUILT_AS.getOrDefault(name, Class.forName(name));
      if (result.getClass() != expected)
        wrong.add(name + " came back as " + result.getClass().getName());
    }
    assertThat(wrong).isEmpty();
  }

  private static StatusRuntimeException withClass(final Status status, final String exceptionClass) {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, exceptionClass);
    return status.asRuntimeException(trailers);
  }
}
