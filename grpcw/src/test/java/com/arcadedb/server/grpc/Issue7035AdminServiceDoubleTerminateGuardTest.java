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

import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Regression test for issue #7035: the {@code responded} guard issue #6756 added to every unary handler of
 * {@link ArcadeDbGrpcService} was never applied to its sibling {@link ArcadeDbGrpcAdminService}, registered on the
 * same server by the same plugin, whose seven handlers kept the pre-fix shape. A client cancel landing between
 * {@code onNext} and {@code onCompleted} makes {@code onCompleted()} throw, and the catch block then called
 * {@code onError} on an already-closed call.
 * <p>
 * Same technique as {@code Issue6756DoubleTerminateGuardTest}: each handler is called directly against a real
 * service bound to the test server, with a mocked observer whose {@code onCompleted()} throws, and {@code onError}
 * must never be invoked afterward.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7035AdminServiceDoubleTerminateGuardTest extends BaseGraphServerTest {
  private static final String CREATED_DATABASE = "Issue7035Created";
  private static final String DROPPED_DATABASE = "Issue7035Dropped";

  private ArcadeDbGrpcAdminService service;

  @BeforeEach
  void setupService() {
    service = new ArcadeDbGrpcAdminService(getServer(0), new DefaultCredentialsValidator());
  }

  @AfterEach
  void dropCreatedDatabases() {
    for (final String name : new String[] { CREATED_DATABASE, DROPPED_DATABASE })
      if (getServer(0).existsDatabase(name)) {
        getServer(0).getDatabase(name).getEmbedded().drop();
        getServer(0).removeDatabase(name);
      }
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private static <T> StreamObserver<T> observerWhoseCompletionThrows() {
    @SuppressWarnings("unchecked")
    final StreamObserver<T> resp = mock(StreamObserver.class);
    doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();
    return resp;
  }

  /**
   * Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw alone does
   * not distinguish "invoked and threw" from "never invoked" - and that the throw was absorbed.
   */
  private static <T> void assertTerminatedOnce(final StreamObserver<T> resp) {
    verify(resp).onNext(any());
    verify(resp).onCompleted();
    verify(resp, never()).onError(any());
  }

  @Test
  void pingDoesNotDoubleTerminateWhenOnCompletedThrows() {
    final StreamObserver<PingResponse> resp = observerWhoseCompletionThrows();
    service.ping(PingRequest.newBuilder().setCredentials(credentials()).build(), resp);
    assertTerminatedOnce(resp);
  }

  @Test
  void getServerInfoDoesNotDoubleTerminateWhenOnCompletedThrows() {
    final StreamObserver<GetServerInfoResponse> resp = observerWhoseCompletionThrows();
    service.getServerInfo(GetServerInfoRequest.newBuilder().setCredentials(credentials()).build(), resp);
    assertTerminatedOnce(resp);
  }

  @Test
  void listDatabasesDoesNotDoubleTerminateWhenOnCompletedThrows() {
    final StreamObserver<ListDatabasesResponse> resp = observerWhoseCompletionThrows();
    service.listDatabases(ListDatabasesRequest.newBuilder().setCredentials(credentials()).build(), resp);
    assertTerminatedOnce(resp);
  }

  @Test
  void existsDatabaseDoesNotDoubleTerminateWhenOnCompletedThrows() {
    final StreamObserver<ExistsDatabaseResponse> resp = observerWhoseCompletionThrows();
    service.existsDatabase(
        ExistsDatabaseRequest.newBuilder().setCredentials(credentials()).setName(getDatabaseName()).build(), resp);
    assertTerminatedOnce(resp);
  }

  @Test
  void createDatabaseDoesNotDoubleTerminateWhenOnCompletedThrows() {
    final StreamObserver<CreateDatabaseResponse> resp = observerWhoseCompletionThrows();
    service.createDatabase(
        CreateDatabaseRequest.newBuilder().setCredentials(credentials()).setName(CREATED_DATABASE).setType("graph").build(),
        resp);
    assertTerminatedOnce(resp);
  }

  @Test
  void dropDatabaseDoesNotDoubleTerminateWhenOnCompletedThrows() {
    getServer(0).createDatabase(DROPPED_DATABASE, ComponentFile.MODE.READ_WRITE);

    final StreamObserver<DropDatabaseResponse> resp = observerWhoseCompletionThrows();
    service.dropDatabase(
        DropDatabaseRequest.newBuilder().setCredentials(credentials()).setName(DROPPED_DATABASE).build(), resp);
    assertTerminatedOnce(resp);
  }

  @Test
  void getDatabaseInfoDoesNotDoubleTerminateWhenOnCompletedThrows() {
    final StreamObserver<GetDatabaseInfoResponse> resp = observerWhoseCompletionThrows();
    service.getDatabaseInfo(
        GetDatabaseInfoRequest.newBuilder().setCredentials(credentials()).setName(getDatabaseName()).build(), resp);
    assertTerminatedOnce(resp);
  }
}
