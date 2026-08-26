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

import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Regression test for issue #6756 (3): {@code executeCommand} with {@code return_rows=true} used to
 * silently drop rows past {@code max_rows} - the extra rows were still counted into {@code affected} but
 * never added to the response, and nothing told the caller the result was incomplete. This diverges from
 * {@code executeQuery}, which fails loudly with {@code RESOURCE_EXHAUSTED} when its own row cap is
 * exceeded. The fix mirrors that: a result exceeding {@code max_rows} now fails with
 * {@code RESOURCE_EXHAUSTED} instead of returning a truncated, unmarked result.
 */
public class Issue6756ExecuteCommandMaxRowsTest extends BaseGraphServerTest {

  private static final String TYPE_NAME = "Issue6756MaxRowsVertex";

  private ArcadeDbGrpcService service;

  @BeforeEach
  void setupService() {
    final String databasePath = getServer(0).getRootPath() + File.separator + "databases";
    service = new ArcadeDbGrpcService(databasePath, getServer(0), 0L, 0L, 0L);

    exec("CREATE VERTEX TYPE " + TYPE_NAME + " IF NOT EXISTS");
    for (int i = 0; i < 10; i++)
      exec("INSERT INTO " + TYPE_NAME + " SET k = " + i);
  }

  @AfterEach
  void teardownService() {
    if (service != null)
      service.close();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private void exec(final String sql) {
    final ExecuteCommandRequest req = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build();
    @SuppressWarnings("unchecked")
    final StreamObserver<ExecuteCommandResponse> resp = mock(StreamObserver.class);
    service.executeCommand(req, resp);
  }

  @Test
  void resultExceedingMaxRowsFailsLoudlyInsteadOfSilentlyTruncating() {
    final ExecuteCommandRequest req = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("SELECT FROM " + TYPE_NAME)
        .setReturnRows(true)
        .setMaxRows(5) // 10 rows exist, only 5 allowed
        .build();

    @SuppressWarnings("unchecked")
    final StreamObserver<ExecuteCommandResponse> resp = mock(StreamObserver.class);

    service.executeCommand(req, resp);

    verify(resp, never()).onNext(any());
    final org.mockito.ArgumentCaptor<Throwable> captor = org.mockito.ArgumentCaptor.forClass(Throwable.class);
    verify(resp).onError(captor.capture());
    assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
    assertThat(((StatusRuntimeException) captor.getValue()).getStatus().getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void resultWithinMaxRowsSucceedsNormally() {
    final ExecuteCommandRequest req = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("SELECT FROM " + TYPE_NAME)
        .setReturnRows(true)
        .setMaxRows(20) // 10 rows exist, well within the cap
        .build();

    @SuppressWarnings("unchecked")
    final StreamObserver<ExecuteCommandResponse> resp = mock(StreamObserver.class);

    service.executeCommand(req, resp);

    verify(resp, never()).onError(any());
    final org.mockito.ArgumentCaptor<ExecuteCommandResponse> captor =
        org.mockito.ArgumentCaptor.forClass(ExecuteCommandResponse.class);
    verify(resp).onNext(captor.capture());
    assertThat(captor.getValue().getRecordsCount()).isEqualTo(10);
  }
}
