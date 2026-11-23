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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #12405 and #12406. The archive that the SQL {@code BACKUP DATABASE '<name>'} statement writes under a name of the caller's
 * choice could not be listed, restored or deleted by the backup commands, which wanted {@code -backup-} in the name, although the
 * reference says an archive written there is visible to all of them (#12405). And every refusal of a restore answered the same
 * label, {@code Cannot execute command}, and a truncated archive answered 500 (#12406).
 */
class Issue12405BackupCommandsAddressEveryArchiveTest extends BaseGraphServerTest {

  @Test
  void anArchiveWithANameOfItsOwnIsListedRestoredAndDeleted() throws Exception {
    final String name = uniqueName("listed");
    final Path archive = backUpAs(name);
    assertThat(archive).exists();

    final Response list = server("list backups " + getDatabaseName());
    assertThat(list.status).as(list.body).isEqualTo(200);
    assertThat(list.body).as("the archive is listed").contains(name);

    final Response restore = server("restore backup " + getDatabaseName() + " " + name + " as restored12405");
    assertThat(restore.status).as(restore.body).isEqualTo(200);
    assertThat(server("list databases").body).as("the restored database exists").contains("restored12405");

    final Response delete = server("delete backup " + getDatabaseName() + " " + name);
    assertThat(delete.status).as(delete.body).isEqualTo(200);
    assertThat(archive).as("the archive is gone").doesNotExist();

    server("drop database restored12405");
  }

  @Test
  void everyRefusalNamesItsReasonInTheLabelAndIsA400() throws Exception {
    final String name = uniqueName("refused");
    final Path archive = backUpAs(name);

    final Response missing = server("restore backup " + getDatabaseName() + " nothere12405.zip as target12405");
    assertThat(missing.status).as(missing.body).isEqualTo(400);
    assertThat(label(missing)).contains("Backup file not found: nothere12405.zip");

    final Response badName = server("restore backup " + getDatabaseName() + " notanarchive.txt as target12405");
    assertThat(badName.status).as(badName.body).isEqualTo(400);
    assertThat(label(badName)).contains("Invalid backup file name: notanarchive.txt");

    final Response taken = server("restore backup " + getDatabaseName() + " " + name + " as " + getDatabaseName());
    assertThat(taken.status).as(taken.body).isEqualTo(400);
    assertThat(label(taken)).contains("already exists").contains("Enable overwrite");

    assertThat(server("list databases").body).as("no database was left behind").doesNotContain("target12405");
    Files.deleteIfExists(archive);
  }

  @Test
  void aTruncatedArchiveIsABadInputNotAServerFault() throws Exception {
    final Path archive = backUpAs(uniqueName("whole"));
    final byte[] whole = Files.readAllBytes(archive);
    final String truncatedName = uniqueName("truncated");
    final Path truncated = archive.resolveSibling(truncatedName);
    Files.write(truncated, Arrays.copyOf(whole, whole.length / 2));

    final Response response = server("restore backup " + getDatabaseName() + " " + truncatedName + " as truncatedtarget12405");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(label(response)).contains("Invalid backup archive");
    assertThat(server("list databases").body).as("nothing is left behind").doesNotContain("truncatedtarget12405");
    Files.deleteIfExists(truncated);
    Files.deleteIfExists(archive);
  }

  @Test
  void aTextFileNamedZipIsABadInputNotAServerFault() throws Exception {
    final Path archive = backUpAs(uniqueName("host"));
    final String fakeName = uniqueName("notzip");
    final Path fake = archive.resolveSibling(fakeName);
    Files.write(fake, "this is not a zip archive".getBytes(StandardCharsets.UTF_8));

    final Response response = server("restore backup " + getDatabaseName() + " " + fakeName + " as notziptarget12405");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(label(response)).contains("Invalid backup archive");
    assertThat(server("list databases").body).as("nothing is left behind").doesNotContain("notziptarget12405");
    Files.deleteIfExists(fake);
    Files.deleteIfExists(archive);
  }

  @Test
  void aCommandMissingItsArgumentsNamesItsUsageInTheLabelAndIsA400() throws Exception {
    final String[][] cases = {
        { "delete backup " + getDatabaseName(), "Usage: delete backup <database> <fileName>" },
        { "restore backup " + getDatabaseName(), "Usage: restore backup <database> <fileName> as <targetDatabase>" },
        { "restore backup " + getDatabaseName() + " as usagetarget12405", "Usage: restore backup <database> <fileName> as <targetDatabase>" },
        { "restore database usagetarget12405", "Usage: restore database <name> <url>" } };

    for (final String[] c : cases) {
      final Response response = server(c[0]);
      assertThat(response.status).as("%s -> %s", c[0], response.body).isEqualTo(400);
      assertThat(label(response)).as(c[0]).isEqualTo(c[1]);
    }
    assertThat(server("list databases").body).as("no database was left behind").doesNotContain("usagetarget12405");
  }

  /** A name no earlier run of the suite used: the backup refuses to overwrite an archive, and the backup folder outlives a test. */
  private static String uniqueName(final String purpose) {
    return purpose + "12405-" + System.nanoTime() + ".zip";
  }

  /** Writes an archive with the SQL statement under a name of the caller's choice and answers its path. */
  private Path backUpAs(final String name) throws Exception {
    final HttpURLConnection connection =
        (HttpURLConnection) new URI(getServerHttpUrl("/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    final Response response = post(connection, new JSONObject().put("language", "sql").put("command", "BACKUP DATABASE '" + name + "'"));
    assertThat(response.status).as(response.body).isEqualTo(200);
    final String file = new JSONObject(response.body).getJSONArray("result").getJSONObject(0).getString("backupFile");
    // the statement answers the name; the archive lives in the database's folder under the server's backup directory
    return Path.of(getServer(0).getRootPath(), "backups", getDatabaseName(), file);
  }

  private static String label(final Response response) {
    return new JSONObject(response.body).getString("error");
  }

  private Response server(final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl("/api/v1/server")).toURL().openConnection();
    return post(connection, new JSONObject().put("command", command));
  }

  private static Response post(final HttpURLConnection connection, final JSONObject payload) throws Exception {
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    final String credentials =
        Base64.getEncoder().encodeToString(("root:" + StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + credentials);
    connection.setDoOutput(true);
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
    }
    try {
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body) {
  }
}
