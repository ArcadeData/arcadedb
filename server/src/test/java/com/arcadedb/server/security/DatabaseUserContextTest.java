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
package com.arcadedb.server.security;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The principal binding that makes the engine's per-user permission gates enforce (GHSA-6x73-v3rc-f57c) runs on
 * pooled worker threads. A bind that is not undone leaks the principal onto whatever request the pool hands that
 * thread next, so runAs must restore the previous binding on every exit path, including a thrown exception.
 */
class DatabaseUserContextTest extends BaseGraphServerTest {

  @Test
  void runAsRestoresThePreviousBindingOnSuccess() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);

    final String seen = DatabaseUserContext.runAs(database, user, () -> {
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
          database.getDatabasePath());
      return context.getCurrentUser() == null ? null : "bound";
    });

    assertThat(seen).isEqualTo("bound");
    assertThat(currentUserOf(database)).isNull();
  }

  @Test
  void runAsRestoresThePreviousBindingWhenTheActionThrows() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);

    try {
      DatabaseUserContext.runAs(database, user, () -> {
        throw new IllegalStateException("boom");
      });
    } catch (final IllegalStateException expected) {
      // EXPECTED
    }

    assertThat(currentUserOf(database)).isNull();
  }

  private SecurityDatabaseUser currentUserOf(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
        database.getDatabasePath());
    return context == null ? null : context.getCurrentUser();
  }
}
