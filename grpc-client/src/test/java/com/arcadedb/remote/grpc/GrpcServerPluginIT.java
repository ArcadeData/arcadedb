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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcServerPluginIT extends BaseGraphServerTest {

  private RemoteGrpcServer   server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void testGrpcQueryWithAliasesAndMetadata() {

    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    database = new RemoteGrpcDatabase(server, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sqlscript", """
        CREATE VERTEX TYPE article IF NOT EXISTS BUCKETS 8;
        CREATE PROPERTY article.id IF NOT EXISTS LONG;
        CREATE PROPERTY article.created IF NOT EXISTS DATETIME;
        CREATE PROPERTY article.updated IF NOT EXISTS DATETIME;
        CREATE PROPERTY article.title IF NOT EXISTS STRING;
        CREATE PROPERTY article.content IF NOT EXISTS STRING;
        CREATE PROPERTY article.author IF NOT EXISTS STRING;
        CREATE PROPERTY article.tags IF NOT EXISTS LIST OF STRING;

        CREATE INDEX IF NOT EXISTS on article(id) UNIQUE;
        """);
    database.command("sqlscript", """
        INSERT INTO article CONTENT {
                "id": 1,
                "created": "2021-01-01 00:00:00",
                "updated": "2021-01-01 00:00:00",
                "title": "My first article",
                "content": "This is the content of my first article",
                "author": "John Doe",
                "tags": ["tag1", "tag2"]
                };
        INSERT INTO article CONTENT {
                "id": 2,
                "created": "2021-01-02 00:00:00",
                "updated": "2021-01-02 00:00:00",
                "title": "My second article",
                "content": "This is the content of my second article",
                "author": "John Doe",
                "tags": ["tag1", "tag3", "tag4"]
                };
        INSERT INTO article CONTENT {
                "id": 3,
                "created": "2021-01-03 00:00:00",
                "updated": "2021-01-03 00:00:00",
                "title": "My third article",
                "content": "This is the content of my third article",
                "author": "John Doe",
                "tags": ["tag2", "tag3"]
                };
        """);

    String query = "SELECT *,  @rid, @type, author AS _author FROM article";
    ResultSet resultSet = database.query("sql", query);

    resultSet.stream().forEach(r -> {
          assertThat(r.<String>getProperty("_author")).isEqualTo("John Doe");
          assertThat(r.getIdentity().get()).isNotNull();
          assertThat(r.getElement().get().getTypeName()).isEqualTo("article");
        }

    );
  }
}
