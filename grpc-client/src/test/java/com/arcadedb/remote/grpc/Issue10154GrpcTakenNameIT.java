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
package com.arcadedb.remote.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.remote.RemoteAlreadyExistsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #10154 and #10153 over gRPC, end to end.
 * <p>
 * #10154: {@code importDatabase} onto a taken name came back as a {@link DuplicatedKeyException} naming an index that does not
 * exist, because the streaming admin calls did not run the taken-name check the unary ones did. #10153: a {@code CREATE} on a
 * taken schema object name came back as {@code INTERNAL} (a {@code CommandExecutionException}) or {@code NOT_FOUND} (a
 * {@code SchemaException}, the opposite of what happened).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10154GrpcTakenNameIT extends BaseGrpcClientServerTest {

  private RemoteGrpcServer   server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // so the URL passes the server's check and the import reaches the creation of the database, which is what refuses it
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(server, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  @Override
  public void endTest() {
    if (database != null) {
      database.close();
      database = null;
    }
    if (server != null) {
      server.close();
      server = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void importingOntoATakenDatabaseNameIsATypedAlreadyExistsException() {
    assertThatThrownBy(() -> server.importDatabase(getDatabaseName(), "file:///does-not-matter-10154.json", null))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isNotInstanceOf(DuplicatedKeyException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining(getDatabaseName());

    // and nothing replaced the database that holds the name
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
  }

  @Test
  void aCreateOnATakenSchemaObjectNameIsAnAlreadyExistsException() {
    database.command("sql", "create vertex type V10154");

    assertThatThrownBy(() -> database.command("sql", "create vertex type V10154"))
        .isInstanceOf(AlreadyExistsException.class)
        .isInstanceOf(SchemaException.class)
        .isNotInstanceOf(DuplicatedKeyException.class)
        .hasMessageContaining("already exists")
        .satisfies(e -> assertThat(((AlreadyExistsException) e).getKind()).isEqualTo(AlreadyExistsException.Kind.TYPE));

    database.command("sql", "create property V10154.p string");
    assertThatThrownBy(() -> database.command("sql", "create property V10154.p string"))
        .isInstanceOf(AlreadyExistsException.class)
        .satisfies(e -> assertThat(((AlreadyExistsException) e).getKind()).isEqualTo(AlreadyExistsException.Kind.PROPERTY));

    database.command("sql", "create materialized view mv10154 as select from V10154");
    assertThatThrownBy(() -> database.command("sql", "create materialized view mv10154 as select from V10154"))
        .isInstanceOf(AlreadyExistsException.class)
        .satisfies(e -> assertThat(((AlreadyExistsException) e).getKind())
            .isEqualTo(AlreadyExistsException.Kind.MATERIALIZED_VIEW));
  }
}
