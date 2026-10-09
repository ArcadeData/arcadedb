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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A real {@link ServerSecurity} over a fresh, empty config directory under {@code target/}, whose calls a test observes
 * and answers (issue #9464). It replaces a Mockito mock of {@link ServerSecurity}.
 * <p>
 * The recorded methods are kept on a {@link CallLog} and answered through {@link #returns}, {@link #fails} and
 * {@link #on}. Overloads share one name and are told apart by the number of arguments they were called with. A method
 * the test has not answered runs the real code against the empty directory: no users, no groups, no replicated
 * fingerprint. That differs from an unstubbed mock, which answered {@code null} or {@code false} and did nothing, so a
 * test that relied on the mock's silence answers the method instead.
 */
public class FakeServerSecurity extends ServerSecurity {
  private static final Set<String> RECORDED = Set.of("authenticate", "revalidate", "getUser", "getUsers",
      "applyReplicatedUsers", "applyReplicatedGroups", "applyReplicatedApiTokens", "seedSecurityStateClusterWide",
      "unconvergedClusterSecurityDocuments");

  private volatile CallLog         log     = new CallLog();
  private final    CallLog.Answers answers = new CallLog.Answers(RECORDED);

  private FakeServerSecurity(final ArcadeDBServer server, final Path configDirectory) {
    super(server, new ContextConfiguration(), configDirectory.toString());
  }

  /** Bound to no server: for code that reads or applies security and never reaches back to a server. */
  public static FakeServerSecurity create() {
    return create(null);
  }

  public static FakeServerSecurity create(final ArcadeDBServer server) {
    final Path configDirectory = TestServerHelper.defaultUnstartedServerRoot().resolve("config");
    try {
      Files.createDirectories(configDirectory);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return new FakeServerSecurity(server, configDirectory);
  }

  /** Records on {@code log} from now on, which other fakes may share. Call it before the fake is used. */
  public FakeServerSecurity recordingOn(final CallLog log) {
    this.log = log;
    return this;
  }

  public CallLog log() {
    return log;
  }

  /** The argument lists of every call to the recorded {@code method}, in arrival order, whatever the overload. */
  public List<List<Object>> calls(final String method) {
    return log.argsOf(this, method);
  }

  /** The recorded {@code method}, in every overload, answers {@code value} from now on. */
  public FakeServerSecurity returns(final String method, final Object value) {
    return on(method, args -> value);
  }

  /** The recorded {@code method}, in every overload, throws {@code failure} from now on. */
  public FakeServerSecurity fails(final String method, final RuntimeException failure) {
    return on(method, args -> {
      throw failure;
    });
  }

  /**
   * The recorded {@code method}, in every overload, runs {@code answer} on its arguments from now on. A {@code void}
   * overload ignores the value returned; {@code args.length} tells the overloads apart.
   */
  public FakeServerSecurity on(final String method, final Function<Object[], Object> answer) {
    answers.set(method, answer);
    return this;
  }

  private Object call(final String method, final Supplier<Object> fallback, final Object... args) {
    // The real constructor runs before this class's fields exist and may call an overridden method: it runs for real
    if (log == null || answers == null)
      return fallback.get();
    log.record(this, method, args);
    final Function<Object[], Object> answer = answers.get(method);
    return answer != null ? answer.apply(args) : fallback.get();
  }

  private static boolean bool(final String method, final Object answer) {
    if (!(answer instanceof Boolean value))
      throw new IllegalStateException("The answer set for '" + method + "' must be a Boolean, it gave " + answer);
    return value;
  }

  @Override
  public ServerSecurityUser authenticate(final String userName, final String userPassword, final String databaseName) {
    return (ServerSecurityUser) call("authenticate", () -> super.authenticate(userName, userPassword, databaseName), userName,
        userPassword, databaseName);
  }

  @Override
  public ServerSecurityUser revalidate(final ServerSecurityUser held) {
    return (ServerSecurityUser) call("revalidate", () -> super.revalidate(held), held);
  }

  @Override
  public ServerSecurityUser getUser(final String userName) {
    return (ServerSecurityUser) call("getUser", () -> super.getUser(userName), userName);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set<String> getUsers() {
    return (Set<String>) call("getUsers", super::getUsers);
  }

  @Override
  public void applyReplicatedUsers(final String usersJsonArray) {
    call("applyReplicatedUsers", () -> {
      super.applyReplicatedUsers(usersJsonArray);
      return null;
    }, usersJsonArray);
  }

  @Override
  public boolean applyReplicatedUsers(final String usersJsonArray, final String expectedFingerprint) {
    return bool("applyReplicatedUsers",
        call("applyReplicatedUsers", () -> super.applyReplicatedUsers(usersJsonArray, expectedFingerprint), usersJsonArray,
            expectedFingerprint));
  }

  @Override
  public void applyReplicatedGroups(final String groupsJson) {
    call("applyReplicatedGroups", () -> {
      super.applyReplicatedGroups(groupsJson);
      return null;
    }, groupsJson);
  }

  @Override
  public boolean applyReplicatedGroups(final String groupsJson, final String expectedFingerprint) {
    return bool("applyReplicatedGroups",
        call("applyReplicatedGroups", () -> super.applyReplicatedGroups(groupsJson, expectedFingerprint), groupsJson,
            expectedFingerprint));
  }

  @Override
  public void applyReplicatedApiTokens(final String apiTokensJson) {
    call("applyReplicatedApiTokens", () -> {
      super.applyReplicatedApiTokens(apiTokensJson);
      return null;
    }, apiTokensJson);
  }

  @Override
  public boolean applyReplicatedApiTokens(final String apiTokensJson, final String expectedFingerprint) {
    return bool("applyReplicatedApiTokens",
        call("applyReplicatedApiTokens", () -> super.applyReplicatedApiTokens(apiTokensJson, expectedFingerprint),
            apiTokensJson, expectedFingerprint));
  }

  /** The no-argument form delegates to {@link #seedSecurityStateClusterWide(long)}, so it is recorded once, there. */
  @Override
  @SuppressWarnings("unchecked")
  public List<String> seedSecurityStateClusterWide(final long retryBudgetMs) {
    return (List<String>) call("seedSecurityStateClusterWide", () -> super.seedSecurityStateClusterWide(retryBudgetMs),
        retryBudgetMs);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> unconvergedClusterSecurityDocuments() {
    return (List<String>) call("unconvergedClusterSecurityDocuments", super::unconvergedClusterSecurityDocuments);
  }
}
