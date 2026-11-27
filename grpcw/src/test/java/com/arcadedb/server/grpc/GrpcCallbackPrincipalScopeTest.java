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
package com.arcadedb.server.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityDatabaseUser;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The callbacks of a client-streaming call after its first chunk run on whichever thread is free, so each applies the user of the
 * stream again ({@link ArcadeDbGrpcService#bindCallbackPrincipal}). This pins which users it binds and which it refuses, for the
 * servers a stream can meet: one with users, which authenticates its callers, and one without, which authenticates nobody.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GrpcCallbackPrincipalScopeTest {
  private static final String DATABASE_PATH = "target/databases/GrpcCallbackPrincipalScopeTest";
  private static final String USER          = "streamuser";

  private Database             database;
  private SecurityDatabaseUser previous;

  @BeforeEach
  void createDatabase() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    database = new DatabaseFactory(DATABASE_PATH).create();
    // what a thread holds from an unrelated request: the scope must give it back
    previous = mock(SecurityDatabaseUser.class);
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(previous);
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aServerWithUsersBindsTheDatabaseUserOfTheStreamAndGivesBackTheOneTheThreadHeld() {
    final ServerSecurityDatabaseUser databaseUser = mock(ServerSecurityDatabaseUser.class);
    final ServerSecurityUser serverUser = mock(ServerSecurityUser.class);
    when(serverUser.getDatabaseUser(database)).thenReturn(databaseUser);
    final ServerSecurity security = securityWithUsers(Set.of(USER));
    when(security.getUser(USER)).thenReturn(serverUser);

    try (final ArcadeDbGrpcService.PrincipalScope ignored = serviceOf(security).bindCallbackPrincipal(database, USER)) {
      assertThat(currentUser()).isSameAs(databaseUser);
    }
    assertThat(currentUser()).isSameAs(previous);
  }

  @Test
  void aServerWithUsersRefusesAStreamWithoutAUserAndLeavesTheThreadAsItWas() {
    final ArcadeDbGrpcService service = serviceOf(securityWithUsers(Set.of(USER)));

    assertThatThrownBy(() -> service.bindCallbackPrincipal(database, null)).isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(Status.fromThrowable(e).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    assertThat(currentUser()).isSameAs(previous);
  }

  @Test
  void aServerWithUsersRefusesAUserItDoesNotKnowAnyMore() {
    final ArcadeDbGrpcService service = serviceOf(securityWithUsers(Set.of(USER)));

    assertThatThrownBy(() -> service.bindCallbackPrincipal(database, "dropped")).isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(Status.fromThrowable(e).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    assertThat(currentUser()).isSameAs(previous);
  }

  @Test
  void aServerWithoutUsersBindsNoUserWhateverTheStreamNames() {
    assertBindsNoUser(serviceOf(securityWithUsers(Collections.emptySet())), USER);
  }

  @Test
  void aServerWithoutUsersBindsNoUserForAStreamWithoutOne() {
    assertBindsNoUser(serviceOf(securityWithUsers(Collections.emptySet())), null);
  }

  @Test
  void aServerWithNoUserListBindsNoUser() {
    assertBindsNoUser(serviceOf(securityWithUsers(null)), USER);
  }

  @Test
  void aServerWithoutSecurityBindsNoUser() {
    assertBindsNoUser(serviceOf(null), USER);
  }

  @Test
  void aServiceWithoutAServerBindsNoUser() {
    assertBindsNoUser(new ArcadeDbGrpcService("target/databases", null, 0L, 0L, 0L), USER);
  }

  private void assertBindsNoUser(final ArcadeDbGrpcService service, final String principal) {
    try (final ArcadeDbGrpcService.PrincipalScope ignored = service.bindCallbackPrincipal(database, principal)) {
      assertThat(currentUser()).as("nobody is authenticated, so no per-user check applies").isNull();
    }
    assertThat(currentUser()).isSameAs(previous);
  }

  private SecurityDatabaseUser currentUser() {
    return DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath()).getCurrentUser();
  }

  private static ServerSecurity securityWithUsers(final Set<String> users) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.getUsers()).thenReturn(users);
    return security;
  }

  private static ArcadeDbGrpcService serviceOf(final ServerSecurity security) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getSecurity()).thenReturn(security);
    return new ArcadeDbGrpcService("target/databases", server, 0L, 0L, 0L);
  }
}
