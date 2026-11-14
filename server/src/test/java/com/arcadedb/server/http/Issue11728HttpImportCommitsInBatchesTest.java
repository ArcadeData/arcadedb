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
package com.arcadedb.server.http;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.PostCommandHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11728: {@code IMPORT DATABASE} sent as a plain {@code POST /api/v1/command} ran inside the transaction the handler opens
 * around every command. The importer took it for its caller's transaction, which it never commits piecemeal (#6561), so the whole
 * import was one transaction held in memory to the end and a large one ran the server out of heap. The handler now leaves that
 * wrapper out for a command that commits its own batches; a caller that asks for it with {@code autoCommit: true} still gets it.
 * <p>
 * Measured by the commits the import makes: one per record of a top-level JSON array when the importer owns the transaction, none
 * when it runs inside one it must leave to its caller.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11728HttpImportCommitsInBatchesTest extends BaseGraphServerTest {
  private static final int  RECORDS = 200;
  private final        File source  = new File("target/issue11728-rows.json");

  @AfterEach
  void deleteSource() {
    source.delete();
  }

  @Test
  void plainCommandImportCommitsItsOwnBatches() throws Exception {
    writeSource();
    final long commits = importAndCountCommits("  import database file://" + source.getAbsolutePath(), null);

    assertThat(commits).as("the import must commit as it goes, not as one transaction at the end").isGreaterThanOrEqualTo(RECORDS);
    assertImported();
  }

  /** Comments the SQL grammar hides do not hide the statement from the handler's probe. */
  @Test
  void importAfterLeadingCommentsCommitsItsOwnBatches() throws Exception {
    writeSource();
    final long commits = importAndCountCommits("/* restore */ -- from the nightly export\n  IMPORT DATABASE file://" + source.getAbsolutePath(),
        null);

    assertThat(commits).as("the import must commit as it goes, not as one transaction at the end").isGreaterThanOrEqualTo(RECORDS);
    assertImported();
  }

  @Test
  void leadingSpaceAndCommentsAreSkippedAsTheGrammarHidesThem() {
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("import")).isEqualTo(0);
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments(" \t\nimport")).isEqualTo(3);
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("/* a */import")).isEqualTo(7);
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("-- a\n/* b */ import")).isEqualTo(13);
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("-- a\rimport")).isEqualTo(5);
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("-- a\r\nimport")).isEqualTo(6);
    // A LINE COMMENT IS TWO DASHES AND A SPACE, AS IN THE LEXER: '--import' IS NOT ONE
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("--import")).isEqualTo(0);
    assertThat(PostCommandHandler.skipLeadingSpaceAndComments("/* never closed import")).isEqualTo(22);
  }

  @Test
  void explicitAutoCommitKeepsTheImportInsideTheRequestTransaction() throws Exception {
    writeSource();
    final long commits = importAndCountCommits("  import database file://" + source.getAbsolutePath(), Boolean.TRUE);

    // THE CALLER ASKED FOR ONE TRANSACTION: THE IMPORT LEAVES IT TO THE HANDLER, WHICH COMMITS IT ONCE AT THE END (#6561)
    assertThat(commits).as("an import inside a transaction the caller asked for commits nothing itself").isLessThan(RECORDS);
    assertImported();
  }

  private long importAndCountCommits(final String command, final Boolean autoCommit) throws Exception {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    final long before = ((Number) database.getStats().get("writeTx")).longValue();

    final HttpURLConnection connection = (HttpURLConnection) URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName()))
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
    if (autoCommit != null)
      payload.put("autoCommit", autoCommit);
    formatPayload(connection, payload);
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).as(response).isEqualTo(200);
      assertThat(new JSONObject(response).getJSONArray("result").getJSONObject(0).getString("result")).isEqualTo("OK");
    } finally {
      connection.disconnect();
    }
    return ((Number) database.getStats().get("writeTx")).longValue() - before;
  }

  private void writeSource() throws Exception {
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < RECORDS; i++)
      json.append(i > 0 ? "," : "").append("{\"k\":").append(i).append('}');
    Files.writeString(source.toPath(), json.append(']').toString(), StandardCharsets.UTF_8);
  }

  private void assertImported() {
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Document", true)).isEqualTo(RECORDS);
  }
}
