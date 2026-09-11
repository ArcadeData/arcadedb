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

import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.server.ServerControlPlane;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A SQL {@code BACKUP DATABASE} or {@code IMPORT DATABASE} refused by the per-database maintenance slot reaches a
 * gRPC client as {@code ABORTED}, not {@code INTERNAL} (issue #7443).
 * <p>
 * The two gRPC surfaces answer it through different code. The admin service's own RPCs - {@code TriggerBackup},
 * {@code RestoreBackup}, {@code RestoreDatabase}, {@code ImportDatabase} - go through
 * {@code ArcadeDbGrpcAdminService.toStatusException}; a SQL statement goes through {@code ExecuteCommand} on the
 * other service and so through {@link GrpcErrorMapper}. Both now match the engine's
 * {@link DatabaseOperationInProgressException}, which is what makes the two agree: the request is well formed and
 * authorized, and retrying once the other operation finishes is the fix.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7443GrpcMaintenanceSlotStatusTest {

  @Test
  void aRefusedSqlMaintenanceStatementMapsToAbortedRatherThanInternal() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationInProgressException("Cannot back up database 'db': a restore of it is already in progress"),
        "ExecuteCommand");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(sre.getStatus().getDescription()).contains("a restore of it is already in progress");
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(DatabaseOperationInProgressException.class.getName());
  }

  /**
   * The server's own control-plane refusal is the same refusal, and it keeps the {@code ABORTED} it has had since
   * #7384 now that its type extends the engine's - on this mapper too, not only on the admin service's.
   */
  @Test
  void theServersOwnRefusalKeepsTheSameStatusThroughTheSameArm() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerControlPlane.OperationInProgressException(
            "Cannot restore database 'db': a backup of it is already in progress"),
        "RestoreDatabase");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
  }
}
