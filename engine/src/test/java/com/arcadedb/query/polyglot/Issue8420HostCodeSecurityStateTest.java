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
package com.arcadedb.query.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityUser;
import org.graalvm.polyglot.PolyglotException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8420: host code reached the live, node-local security state of the request through the bound {@code database}
 * object - {@code database.getContext().getCurrentUser()} returned the real user, whose public mutators change its cached
 * permissions, and {@code database.getContext().setCurrentUser(...)} replaced it. The polyglot sandbox now denies the
 * thread context and the user types to scripts, whichever path hands them out.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8420HostCodeSecurityStateTest extends TestHelper {

  /**
   * A {@code LANGUAGE js} function runs on the caller's thread, inside the request it was called from - the path on
   * which {@code database.getContext()} is the request's own context and a replaced user outlives the script.
   */
  @Test
  void functionCannotReplaceOrReadTheCurrentUser() {
    database.command("sql", "DEFINE FUNCTION sec.dropUser 'database.getContext().setCurrentUser(null); return 1;' LANGUAGE js");
    database.command("sql", "DEFINE FUNCTION sec.readUser 'return database.getContext().getCurrentUser().getName();' LANGUAGE js");

    final DatabaseContext.DatabaseContextTL context = ((DatabaseInternal) database).getContext();
    final SecurityDatabaseUser user = proxy(SecurityDatabaseUser.class, new AtomicInteger());
    context.setCurrentUser(user);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT `sec.dropUser`() AS r")) {
          rs.next();
        }
      }).as("a function must not be able to replace the user of the request it runs in").isNotNull();
      assertThat(context.getCurrentUser()).isSameAs(user);

      // WITHOUT THE SANDBOX THIS ANSWERS THE USER'S NAME: THE SCRIPT HOLDS THE LIVE USER OBJECT AND ITS MUTATORS
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT `sec.readUser`() AS r")) {
          rs.next();
        }
      }).as("a function must not be able to reach the user of the request it runs in").isNotNull();
    } finally {
      context.setCurrentUser(null);
    }
  }

  @Test
  void scriptCannotCallAUserMutatorWhateverPathHandsTheUserOut() {
    final AtomicInteger calls = new AtomicInteger();
    final SecurityDatabaseUser databaseUser = proxy(SecurityDatabaseUser.class, calls);
    final SecurityUser serverUser = proxy(SecurityUser.class, calls);

    assertDenied("user.getName();", Map.of("user", databaseUser));
    assertDenied("user.getName();", Map.of("user", serverUser));

    assertThat(calls.get()).as("no user method may have been invoked by the script").isZero();
  }

  @Test
  void theRestOfTheDatabaseApiIsStillReachable() {
    // POSITIVE CONTROL: ONLY THE SECURITY STATE IS WALLED OFF, NOT THE BOUND DATABASE OBJECT
    try (final ResultSet rs = database.command("js", "database.getName();")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo(database.getName());
    }
  }

  private void assertDenied(final String script) {
    assertDenied(script, Map.of());
  }

  private void assertDenied(final String script, final Map<String, Object> parameters) {
    assertThatThrownBy(() -> database.command("js", script, parameters))
        .isInstanceOf(CommandExecutionException.class)
        .hasCauseInstanceOf(PolyglotException.class);
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(final Class<T> type, final AtomicInteger calls) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, args) -> {
      // COUNTED ONLY FOR THE CALLS A SCRIPT MAKES: THE ENGINE'S OWN PERMISSION CHECKS ARE ANSWERED "ALLOWED"
      if (method.getReturnType() == boolean.class)
        return true;
      calls.incrementAndGet();
      return method.getReturnType() == String.class ? "someone" : null;
    });
  }
}
