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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseAlreadyExistsException;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.UserAlreadyExistsException;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10075: the Java gRPC client reads a trailer-less {@code ALREADY_EXISTS} from any admin RPC as "the name is taken"
 * and raises {@code RemoteAlreadyExistsException}. That is only sound while the admin service keeps the convention this
 * test pins: it answers {@code ALREADY_EXISTS} for a taken user, token or database name and for nothing else, and it sends
 * those without an exception-class trailer. A new admin conflict that needs the same status has to name itself in the
 * trailer, or this test goes red before the client mislabels it.
 */
class Issue10075AdminAlreadyExistsMeansATakenNameTest {
  @TempDir
  Path root;

  @Test
  void everyTakenNameIsATrailerlessAlreadyExists() {
    final ArcadeDbGrpcAdminService service = adminService();
    for (final Exception taken : List.of(new UserAlreadyExistsException("bob"),
        new ServerControlPlane.AlreadyExistsException("A token with name 'ci' already exists"),
        new DatabaseAlreadyExistsException("Database 'db' already exists"))) {
      final StatusException status = service.toStatus("create", taken);

      assertThat(status.getStatus().getCode()).as(taken.getClass().getSimpleName()).isEqualTo(Status.Code.ALREADY_EXISTS);
      final Metadata trailers = status.getTrailers();
      assertThat(trailers == null ? null : trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
          .as("the client recognises a taken name by the missing trailer").isNull();
    }
  }

  @Test
  void nothingElseTheAdminServiceRaisesIsAlreadyExists() {
    final ArcadeDbGrpcAdminService service = adminService();
    for (final Exception other : List.of(new IllegalArgumentException("Database 'x' already exists"),
        new ServerSecurityException("not allowed"), new ServerControlPlane.NotFoundException("User 'bob' not found"),
        new DatabaseOperationInProgressException("backup running"), new DuplicatedKeyException("idx", "[k]", null),
        new RuntimeException("boom"))) {
      final StatusException status = service.toStatus("op", other);

      // a conflict that does share the status must name itself, so the client keeps its type
      if (status.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        assertThat(status.getTrailers()).as(other.getClass().getSimpleName()).isNotNull();
        assertThat(status.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).as(other.getClass().getSimpleName()).isNotNull();
      }
    }
  }

  private ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
