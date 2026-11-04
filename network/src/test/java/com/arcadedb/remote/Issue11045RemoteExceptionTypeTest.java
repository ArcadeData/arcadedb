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
package com.arcadedb.remote;

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ExceptionPackageCensus;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.index.IndexException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.query.opencypher.parser.FeatureNotImplementedException;
import com.arcadedb.schema.trigger.TriggerExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.LockException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11045: the HTTP driver rebuilt the server's exception from a table of twenty names with no entry for the command
 * family, so a SQL syntax error reached a remote caller as a {@link RemoteException}, which was not an
 * {@link ArcadeDBException}: a {@code catch (ArcadeDBException)} that caught it embedded let it escape over the driver.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11045RemoteExceptionTypeTest {
  /** The few that cannot come back as themselves, with what they come back as. */
  private static final Map<String, Class<?>> REBUILT_AS = Map.of(
      // its (String) constructor takes a type name: rebuilt as the AlreadyExistsException it extends (issue #10153)
      TypeAlreadyExistsException.class.getName(), AlreadyExistsException.class,
      // its constructor takes the names of the missing parameters: rebuilt as the semantic error it extends
      CommandParameterMissingException.class.getName(), CommandSemanticException.class,
      // rebuilt from its exceptionArgs, which this body does not carry (issue #9473, see RemoteHttpComponentTest)
      DuplicatedKeyException.class.getName(), RemoteException.class);

  private RemoteHttpComponentTest.TestableRemoteHttpComponent component;

  @BeforeEach
  void setUp() {
    // Nothing is sent: manageException only reads the body it is given
    component = new RemoteHttpComponentTest.TestableRemoteHttpComponent("localhost", 1, "root", "test");
  }

  @AfterEach
  void tearDown() {
    component.close();
  }

  @Test
  void theCommandFamilyKeepsItsType() {
    assertRebuiltAs(CommandSQLParsingException.class, 400, "SQL syntax error at line 1, column 0");
    assertRebuiltAs(CommandParsingException.class, 400, "Cannot parse the command");
    assertRebuiltAs(CommandSemanticException.class, 400, "Variable 'x' not defined");
    assertRebuiltAs(CommandExecutionException.class, 500, "Bucket 'bucket:nosuchbucket' does not exist");
    assertRebuiltAs(QueryNotIdempotentException.class, 500, "Query 'insert into Doc set a = 1' is not idempotent");
    // An ArcadeDB exception outside com.arcadedb.exception is rebuilt too
    assertRebuiltAs(LockException.class, 500, "Cannot acquire the lock");
  }

  @Test
  void anUnknownQueryLanguageIsAnIllegalArgumentExceptionAsItIsEmbedded() {
    final Exception result = component.manageException(400, body(IllegalArgumentException.class.getName(),
        "Query engine 'nosuchlang' was not found. Check your configuration"), "command");

    assertThat(result).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Query engine 'nosuchlang' was not found. Check your configuration");
  }

  /** A type this client cannot rebuild still reaches a portable handler, and leads with the server's own sentence. */
  @Test
  void aTypeThisClientCannotRebuildIsStillAnArcadeDBException() {
    final Exception result = component.manageException(500, body("com.arcadedb.plugin.SomePluginException", "The plugin refused"),
        "command");

    assertThat(result).isExactlyInstanceOf(RemoteException.class).isInstanceOf(ArcadeDBException.class);
    assertThat(result.getMessage()).startsWith("Error on executing remote command 'command': The plugin refused")
        .contains("com.arcadedb.plugin.SomePluginException");
  }

  /** The name is the server's: nothing it names is instantiated unless the table lists it. */
  @Test
  void onlyListedNamesAreRebuilt() {
    for (final String name : new String[] { "com.arcadedb.remote.RemoteDatabase", "com.arcadedb.database.RID",
        "java.lang.Runtime", "java.lang.IllegalStateException", "com.arcadedb.exception.ArcadeDBException$Nope", "" }) {
      assertThat(RemoteExceptionRebuilder.rebuild(name, "x")).as(name).isNull();
      assertThat(component.manageException(500, body(name, "x"), "command")).as(name).isExactlyInstanceOf(RemoteException.class);
    }
    assertThat(RemoteExceptionRebuilder.rebuild(null, "x")).isNull();
  }

  /**
   * The guarantee: every concrete exception of the engine's exception package comes back as itself over the driver, so the
   * mapping cannot drift again the next time the engine gains one. The few that cannot are listed with what they come back as.
   */
  @Test
  void everyExceptionOfTheEnginePackageComesBackAsItself() throws Exception {
    final List<String> wrong = new ArrayList<>();
    for (final Class<?> type : ExceptionPackageCensus.concreteExceptions()) {
      final Exception result = component.manageException(500, body(type.getName(), "boom"), "command");
      final Class<?> expected = REBUILT_AS.getOrDefault(type.getName(), type);
      if (result.getClass() != expected)
        wrong.add(type.getName() + " came back as " + result.getClass().getName());
    }
    assertThat(wrong).isEmpty();
  }

  /**
   * Issue #11198: the census above reads a package, the table draws from several. This one reads the authority's own key set,
   * so an entry cannot leave {@link RemoteExceptionRebuilder} without a test noticing, wherever its class lives: every name it
   * holds comes back over the driver as the class it names (or as the one it is documented to be rebuilt as).
   */
  @Test
  void everyNameOfTheAuthorityComesBackAsItself() throws Exception {
    final List<String> wrong = new ArrayList<>();
    for (final String name : RemoteExceptionRebuilder.names()) {
      final Exception result = component.manageException(500, body(name, "boom"), "command");
      final Class<?> expected = REBUILT_AS.getOrDefault(name, Class.forName(name));
      if (result.getClass() != expected)
        wrong.add(name + " came back as " + result.getClass().getName());
    }
    assertThat(wrong).isEmpty();
  }

  /**
   * Issue #11198: the entries whose class is not in the engine's exception package are pinned class by class, so deleting one
   * (or adding one without saying why it is there) fails here instead of turning a remote exception back into a
   * {@link RemoteException} unseen.
   */
  @Test
  void theNamesOutsideTheEnginePackageArePinned() {
    final String enginePackage = ArcadeDBException.class.getPackageName() + ".";
    final Set<String> outside = new TreeSet<>();
    for (final String name : RemoteExceptionRebuilder.names())
      if (!name.startsWith(enginePackage) || name.indexOf('.', enginePackage.length()) >= 0)
        outside.add(name);

    assertThat(outside).containsExactlyInAnyOrder(FunctionExecutionException.class.getName(), IndexException.class.getName(),
        QuorumNotReachedException.class.getName(), FeatureNotImplementedException.class.getName(),
        TriggerExecutionException.class.getName(), LockException.class.getName(), IllegalArgumentException.class.getName(),
        NoSuchElementException.class.getName(), SecurityException.class.getName());
  }

  /** A 503 keeps the retry contract: a rebuilt type has to be retryable, or a NeedRetryException comes back instead. */
  @Test
  void a503StaysRetryable() {
    assertThat(component.manageException(503, body(LockTimeoutException.class.getName(), "Timeout on locking"), "command"))
        .isExactlyInstanceOf(LockTimeoutException.class);
    assertThat(component.manageException(503, body(CommandExecutionException.class.getName(), "Transient"), "command"))
        .isExactlyInstanceOf(NeedRetryException.class);
  }

  /** The streamed encoding goes through the same mapping, so it gains the same types. */
  @Test
  void aStreamedErrorLineKeepsTheTypeToo() {
    final RuntimeException mapped = component.manageStreamedError(new JSONObject().put("status", 400)
        .put("exception", CommandSQLParsingException.class.getName()).put("message", "SQL syntax error"), "query");

    assertThat(mapped).isExactlyInstanceOf(CommandSQLParsingException.class).hasMessage("SQL syntax error");
  }

  private void assertRebuiltAs(final Class<? extends ArcadeDBException> type, final int status, final String detail) {
    final Exception result = component.manageException(status, body(type.getName(), detail), "command");
    assertThat(result).as(type.getName()).isExactlyInstanceOf(type).hasMessage(detail);
  }

  private static String body(final String exception, final String detail) {
    return new JSONObject().put("error", "Cannot execute command").put("detail", detail).put("exception", exception).toString();
  }
}
