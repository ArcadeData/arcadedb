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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Issue #12801: {@code create database} answered HTTP 500 for a name the caller got wrong when the name held a {@code *} (the engine
 * refuses it after the server's own check let it through) or was longer than a file name can be (the directory cannot be created).
 * Every other refused name answers 400, and so must these.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12801CreateDatabaseNameStatusIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @ParameterizedTest
  @ValueSource(strings = { "star*name", "*", "trailing*" })
  void aNameWithAStarIsRefusedAsABadRequest(final String name) throws Exception {
    final HttpResponse<String> response = executeServerCommand("create database " + name);

    assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(400);
    assertThat(executeServerCommand("list databases").body()).doesNotContain(name);
  }

  @Test
  void aNameLongerThanAFileNameIsRefusedAsABadRequest() throws Exception {
    final String name = "x".repeat(256);

    final HttpResponse<String> response = executeServerCommand("create database " + name);

    assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(400);
    assertThat(response.body()).as("the reply does not echo the whole path").doesNotContain(name);
  }

  /**
   * 100 characters, 300 bytes in UTF-8: past what a file name holds where the limit is in bytes (ext4, XFS) and within it
   * where it is in characters (APFS, NTFS). So the file system decides, and the server only never calls its refusal a server fault.
   */
  @Test
  void aNameOfMultiByteCharactersIsCreatedOrRefusedAsABadRequestAsTheFileSystemDecides() throws Exception {
    final String name = "\u65e5".repeat(100);

    final HttpResponse<String> response = executeServerCommand("create database " + name);
    try {
      assertThat(response.statusCode()).as("body=%s", response.body()).isIn(200, 400);
      if (response.statusCode() == 400)
        assertThat(response.body()).as("the reply does not echo the whole path").doesNotContain(name);
    } finally {
      if (response.statusCode() == 200)
        executeServerCommand("drop database " + name);
    }
  }

  /**
   * The name check is also the gate of every open, backup and drop, so it must not carry a length limit of its own: a database
   * whose name its file system accepted (APFS and NTFS take these 300 bytes) would no longer be opened, backed up or dropped.
   */
  @Test
  void theNameCheckSharedWithOpenAndDropHasNoLengthLimitOfItsOwn() {
    assertThatCode(() -> getServer(0).checkDatabaseNameIsValid("\u65e5".repeat(100))).doesNotThrowAnyException();
    assertThatCode(() -> getServer(0).checkDatabaseNameIsValid("y".repeat(300))).doesNotThrowAnyException();
  }

  /**
   * The console, gRPC, open, backup and drop go through the same check without the HTTP handler in front of it: the refusal of a
   * {@code *} is the check's own, and it is the caller's mistake there too.
   */
  @ParameterizedTest
  @ValueSource(strings = { "a*b", "*", "trailing*" })
  void theSharedNameCheckRefusesAStar(final String name) {
    assertThatThrownBy(() -> getServer(0).checkDatabaseNameIsValid(name)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid characters");
  }

  /**
   * A long name is not what failed when the database directory takes no new directory at all: that stays a server fault (500),
   * whatever the length of the name.
   */
  @Test
  void aLongNameIsNotBlamedWhenTheDatabaseDirectoryTakesNoNewDirectory() throws Exception {
    final File databaseDirectory = new File(
        getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    assumeThat(databaseDirectory.setWritable(false)).as("the directory can be made read-only").isTrue();
    try {
      // Whoever ignores the permission (root) still creates the database, and there is no failure to classify
      assumeThat(Files.isWritable(databaseDirectory.toPath())).isFalse();

      for (final String name : new String[] { "x".repeat(256), "\u65e5".repeat(100) }) {
        final HttpResponse<String> response = executeServerCommand("create database " + name);

        assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(500);
      }
    } finally {
      databaseDirectory.setWritable(true);
    }
  }

  @Test
  void aLongNameWithinEveryFileSystemLimitStillWorks() throws Exception {
    final String name = "y".repeat(100);
    try {
      assertThat(executeServerCommand("create database " + name).statusCode()).isEqualTo(200);
    } finally {
      executeServerCommand("drop database " + name);
    }
  }

  private HttpResponse<String> executeServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
