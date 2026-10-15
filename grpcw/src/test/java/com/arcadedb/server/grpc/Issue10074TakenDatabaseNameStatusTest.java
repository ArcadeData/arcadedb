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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseAlreadyExistsException;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10074 on the gRPC transport. {@code CreateDatabase} already answered {@code ALREADY_EXISTS} from its own
 * pre-check, but {@code ImportDatabase} creates the database through the same server call HTTP's `create database` does
 * and surfaced the refusal as a plain {@link IllegalArgumentException}: {@code INVALID_ARGUMENT}, the status of a
 * malformed request. The conflict is its own type now, and this transport maps it the way it maps a taken token name.
 * A plain {@link IllegalArgumentException} must keep meaning {@code INVALID_ARGUMENT}: the restore routes still raise
 * one for a taken target, and issue #7395 pins that.
 */
class Issue10074TakenDatabaseNameStatusTest {
  @TempDir
  Path root;

  @Test
  void aTakenDatabaseNameIsAlreadyExists() {
    final Status status = adminService().toStatus("importDatabase", new DatabaseAlreadyExistsException("Database 'x' already exists"))
        .getStatus();

    assertThat(status.getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(status.getDescription()).contains("already exists");
  }

  @Test
  void aPlainIllegalArgumentStaysInvalidArgument() {
    assertThat(adminService().toStatus("restoreDatabase", new IllegalArgumentException("Database 'x' already exists")).getStatus()
        .getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  private ArcadeDbGrpcAdminService adminService() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, new ContextConfiguration());
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
