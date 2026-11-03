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
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.index.IndexException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.query.opencypher.parser.FeatureNotImplementedException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteExceptionRebuilder;
import com.arcadedb.schema.trigger.TriggerExecutionException;
import com.arcadedb.utility.LockException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11045, the gRPC side: the client rebuilt the server's exception from its own table of names, with the command family
 * absent, and fell back to the status code, which reads none of it. No server is required.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11045GrpcCommandExceptionTypeTest {

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
    final Map<String, Class<?>> rebuiltAs = Map.of(
        // its (String) constructor takes a type name: rebuilt as the AlreadyExistsException it extends (issue #10153)
        TypeAlreadyExistsException.class.getName(), AlreadyExistsException.class,
        // its constructor takes the names of the missing parameters: rebuilt as the semantic error it extends
        CommandParameterMissingException.class.getName(), CommandSemanticException.class);

    final List<String> wrong = new ArrayList<>();
    for (final Class<?> type : concreteExceptionsOfTheEnginePackage()) {
      final RuntimeException result = GrpcClientErrorMapper.toException(
          withClass(Status.INTERNAL.withDescription("boom"), type.getName()));
      final Class<?> expected = rebuiltAs.getOrDefault(type.getName(), type);
      if (result.getClass() != expected)
        wrong.add(type.getName() + " came back as " + result.getClass().getName());
    }
    assertThat(wrong).isEmpty();
  }

  /**
   * The census enumerates from the authority, not from a package (issue #11199): every name {@link RemoteExceptionRebuilder}
   * lists comes back as itself, wherever its class lives, and the table is asserted to still hold the names no package scan
   * reaches, so deleting one of those entries is a red build.
   */
  @Test
  void everyNameOfTheRebuilderTableComesBackAsItself() throws Exception {
    final Set<String> names = RemoteExceptionRebuilder.rebuildableNames();
    assertThat(names).as("the census must still reach the RemoteExceptionRebuilder table, including the exceptions that live "
        + "outside com.arcadedb.exception").contains(LockException.class.getName(), IndexException.class.getName(),
        QuorumNotReachedException.class.getName(), FeatureNotImplementedException.class.getName(),
        TriggerExecutionException.class.getName(), FunctionExecutionException.class.getName(),
        IllegalArgumentException.class.getName(), ArcadeDBException.class.getName());

    // its constructor takes the names of the missing parameters, not a message: the table rebuilds it as the semantic error it is
    final Map<String, Class<?>> rebuiltAs = Map.of(CommandParameterMissingException.class.getName(),
        CommandSemanticException.class);
    final List<String> wrong = new ArrayList<>();
    for (final String name : names) {
      final Class<?> expected;
      try {
        expected = rebuiltAs.getOrDefault(name, Class.forName(name));
      } catch (final ClassNotFoundException e) {
        wrong.add(name + " is listed in the table but its class does not exist");
        continue;
      }
      final Class<?> actual = GrpcClientErrorMapper.toException(withClass(Status.INTERNAL.withDescription("boom"), name)).getClass();
      if (actual != expected)
        wrong.add(name + " came back as " + actual.getName());
    }
    assertThat(wrong).isEmpty();
  }

  private static List<Class<?>> concreteExceptionsOfTheEnginePackage() throws Exception {
    final String packagePath = ArcadeDBException.class.getPackageName().replace('.', '/');
    final URL location = ArcadeDBException.class.getProtectionDomain().getCodeSource().getLocation();
    final List<String> classNames = new ArrayList<>();
    final Path root = Path.of(location.toURI());
    if (Files.isDirectory(root)) {
      try (final Stream<Path> files = Files.list(root.resolve(packagePath))) {
        files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".class") && !n.contains("$"))
            .forEach(n -> classNames.add(n.substring(0, n.length() - ".class".length())));
      }
    } else {
      try (final JarFile jar = new JarFile(root.toFile())) {
        jar.stream().map(JarEntry::getName)
            .filter(n -> n.startsWith(packagePath + "/") && n.endsWith(".class") && !n.contains("$") && n.indexOf('/',
                packagePath.length() + 1) < 0)
            .forEach(n -> classNames.add(n.substring(packagePath.length() + 1, n.length() - ".class".length())));
      }
    }

    final List<Class<?>> types = new ArrayList<>();
    for (final String simpleName : classNames) {
      final Class<?> type = Class.forName(ArcadeDBException.class.getPackageName() + "." + simpleName);
      if (ArcadeDBException.class.isAssignableFrom(type) && Modifier.isPublic(type.getModifiers()) && !Modifier.isAbstract(
          type.getModifiers()))
        types.add(type);
    }
    assertThat(types).as("the scan found the package").hasSizeGreaterThan(30);
    return types;
  }

  private static StatusRuntimeException withClass(final Status status, final String exceptionClass) {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, exceptionClass);
    return status.asRuntimeException(trailers);
  }
}
