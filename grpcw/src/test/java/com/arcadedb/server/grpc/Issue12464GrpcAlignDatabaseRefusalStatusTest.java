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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.parser.AlignDatabaseStatement;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #12464: since #11796 the {@code ALIGN DATABASE} statement refuses with a {@link CommandSemanticException}, which HTTP
 * answers 400. The admin RPC {@code AlignDatabase} runs that statement through {@code ServerControlPlane.alignDatabase}, and
 * {@code ArcadeDbGrpcAdminService.toStatus} had no arm for a refused statement, so it answered {@code INTERNAL} - a server
 * fault - and in production mode concealed the sentence that says why. The refusal is the caller's, as the data plane's
 * {@code ExecuteCommand} already answers the same statement ({@code ErrorCategory.PARSING}): {@code INVALID_ARGUMENT}, with
 * the ArcadeDB-authored sentence kept in production mode as HTTP keeps it.
 * <p>
 * The failure is the one a real database raises, not one built by hand, so the test follows the chain the engine actually
 * puts around the refusal.
 */
class Issue12464GrpcAlignDatabaseRefusalStatusTest {
  @TempDir
  Path root;

  @Test
  void theAlignDatabaseRefusalIsARejectedRequestAndNotAServerFault() {
    final Exception refusal = alignDatabaseRefusal();

    final StatusException status = adminService(false).toStatus("alignDatabase", refusal);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getStatus().getDescription()).contains(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED);
  }

  @Test
  void productionModeKeepsTheSentenceOfTheAlignDatabaseRefusal() {
    final Exception refusal = alignDatabaseRefusal();

    final StatusException status = adminService(true).toStatus("alignDatabase", refusal);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getStatus().getDescription())
        .as("the refusal is a sentence ArcadeDB wrote about the request, which HTTP keeps in production mode too")
        .contains(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED)
        .doesNotContain(GrpcErrorMapper.CONCEALED_DESCRIPTION);
  }

  /** The failure {@code ServerControlPlane.alignDatabase} propagates: the statement it runs, refused by a real database. */
  private Exception alignDatabaseRefusal() {
    try (final DatabaseFactory factory = new DatabaseFactory(root.resolve("align").toString());
        final Database database = factory.create()) {
      try (final var rs = database.command("sql", "align database")) {
        fail("ALIGN DATABASE is refused by every database (issue #11796), but it ran");
      } catch (final CommandSemanticException e) {
        assertThat(e).hasMessageContaining(AlignDatabaseStatement.ALIGN_DATABASE_NOT_SUPPORTED);
        return e;
      }
      throw new AssertionError("unreachable");
    }
  }

  private ArcadeDbGrpcAdminService adminService(final boolean production) {
    final ContextConfiguration configuration = new ContextConfiguration();
    if (production)
      configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root.resolve(production ? "production" : "development"),
        configuration);
    return new ArcadeDbGrpcAdminService(server, new DefaultCredentialsValidator());
  }
}
