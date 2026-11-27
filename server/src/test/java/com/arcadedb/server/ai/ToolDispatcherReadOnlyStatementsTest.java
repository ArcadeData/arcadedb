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
package com.arcadedb.server.ai;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code query_database} tool of the assistant runs statements that declare a read and nothing else. A statement that declares
 * another operation (the backup statement) is answered with an error and runs nothing, in every language that can wrap it, and
 * plain reads are still answered.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ToolDispatcherReadOnlyStatementsTest extends BaseGraphServerTest {
  private File archiveDirectory;

  @Override
  protected void onServerConfiguration(final ContextConfiguration serverConfiguration) {
    super.onServerConfiguration(serverConfiguration);
    archiveDirectory = new File("./target/tool-dispatcher-read-only-archives");
    FileUtils.deleteRecursively(archiveDirectory);
    archiveDirectory.mkdirs();
    serverConfiguration.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, archiveDirectory.getAbsolutePath());
  }

  @AfterEach
  void removeArchives() {
    if (archiveDirectory != null)
      FileUtils.deleteRecursively(archiveDirectory);
  }

  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  private JSONObject queryDatabase(final String language, final String command) {
    return new JSONObject(new ToolDispatcher(getServer(0), rootUser(), getDatabaseName()).execute("query_database",
        new JSONObject().put("language", language).put("command", command)));
  }

  private long writtenFiles() throws IOException {
    final Path root = archiveDirectory.toPath();
    if (!Files.exists(root))
      return 0L;
    try (final Stream<Path> files = Files.walk(root)) {
      return files.filter(Files::isRegularFile).count();
    }
  }

  @Test
  void aStatementDeclaringAnotherOperationThanAReadIsRefusedWhateverTheLanguage() throws IOException {
    final String[][] statements = {//
        { "sql", "BACKUP DATABASE" },//
        { "sqlscript", "BACKUP DATABASE;" },//
        { "sqlscript", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1; BACKUP DATABASE;" } };

    for (final String[] statement : statements)
      assertThat(queryDatabase(statement[0], statement[1]).has("error")).as("%s: %s", statement[0], statement[1]).isTrue();

    assertThat(writtenFiles()).isZero();
  }

  @Test
  void plainReadsAreStillAnswered() {
    for (final String[] statement : new String[][] {//
        { "sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1" },//
        { "sqlscript", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1; SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1;" },//
        { "cypher", "MATCH (n:" + VERTEX1_TYPE_NAME + ") RETURN n LIMIT 1" },//
        { "sql", "EXPLAIN SELECT FROM " + VERTEX1_TYPE_NAME } }) {
      final JSONObject result = queryDatabase(statement[0], statement[1]);
      assertThat(result.has("error")).as("%s: %s -> %s", statement[0], statement[1], result).isFalse();
      assertThat(result.getJSONArray("result").length()).as("%s: %s", statement[0], statement[1]).isGreaterThan(0);
    }
  }
}
