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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12417: {@code restore backup} is forwarded to the leader, while {@code trigger backup}, {@code list backups}
 * and {@code delete backup} ran on whichever node the request landed on. An archive triggered through a follower was
 * written into that follower's own backup directory, from that follower's own copy of the database, and no restore
 * could reach it: the leader answered 400 because its directory did not hold the file.
 * <p>
 * Every server of this cluster has its own backup directory, as separate hosts do. The whole family is asked of a
 * follower and must act on the leader's directory, the one the restore reads.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12417BackupCommandsOnFollowerIT extends BaseRaftHATest {

  private static final String BACKUP_ROOT = "./target/backups-issue12417";
  private static final String RESTORED    = "issue12417_restored";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final File directory = new File(BACKUP_ROOT, "server" + serverIndexOf(config));
    FileUtils.deleteRecursively(directory);
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, directory.getPath());
  }

  @AfterEach
  void dropRestoredDatabaseAndArchives() throws Exception {
    try {
      final int leader = findLeaderIndex();
      if (leader >= 0 && getServer(leader).existsDatabase(RESTORED)) {
        postCommand(leader, "drop database " + RESTORED);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> !existsOnAnyServer(RESTORED));
      }
    } finally {
      FileUtils.deleteRecursively(new File(BACKUP_ROOT));
    }
  }

  @Test
  void anArchiveTriggeredThroughAFollowerIsListedRestoredAndDeletedThroughIt() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    waitForAllServers();

    final String database = getDatabaseName();
    final Path leaderDirectory = backupDirectoryOf(leader).resolve(database);
    final Path followerDirectory = backupDirectoryOf(follower).resolve(database);
    assertThat(leaderDirectory).as("the two servers must not share a backup directory, or the test proves nothing")
        .isNotEqualTo(followerDirectory);

    final HttpResponse<String> trigger = postCommand(follower, "trigger backup " + database);
    assertThat(trigger.statusCode()).as(trigger.body()).isEqualTo(200);
    final String archive = new File(new JSONObject(trigger.body()).getString("backupFile")).getName();

    assertThat(leaderDirectory.resolve(archive)).as("the archive is written by the leader, where the restore reads it")
        .isRegularFile();
    assertThat(followerDirectory.resolve(archive)).as("the follower writes no archive of its own").doesNotExist();

    // The listing a follower answers is the listing the restore can act on
    for (final int server : new int[] { follower, leader }) {
      final HttpResponse<String> list = postCommand(server, "list backups " + database);
      assertThat(list.statusCode()).as(list.body()).isEqualTo(200);
      assertThat(fileNamesOf(list.body())).as("list backups on server %d", server).containsExactly(archive);
    }

    final HttpResponse<String> restore = postCommand(follower, "restore backup " + database + " " + archive + " as " + RESTORED);
    assertThat(restore.statusCode()).as(restore.body()).isEqualTo(200);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!getServer(i).existsDatabase(RESTORED))
          return false;
      return true;
    });

    final HttpResponse<String> delete = postCommand(follower, "delete backup " + database + " " + archive);
    assertThat(delete.statusCode()).as(delete.body()).isEqualTo(200);
    assertThat(leaderDirectory.resolve(archive)).as("delete backup removes the archive the listing named").doesNotExist();

    final HttpResponse<String> listAfterDelete = postCommand(follower, "list backups " + database);
    assertThat(fileNamesOf(listAfterDelete.body())).isEmpty();
  }

  private Path backupDirectoryOf(final int serverIndex) {
    return new ServerControlPlane(getServer(serverIndex)).resolveBackupDirectory();
  }

  private boolean existsOnAnyServer(final String database) {
    for (int i = 0; i < getServerCount(); i++)
      if (getServer(i).existsDatabase(database))
        return true;
    return false;
  }

  private static String[] fileNamesOf(final String listing) {
    final JSONArray backups = new JSONObject(listing).getJSONArray("backups");
    final String[] names = new String[backups.length()];
    for (int i = 0; i < names.length; i++)
      names[i] = backups.getJSONObject(i).getString("fileName");
    return names;
  }

  private HttpResponse<String> postCommand(final int serverIndex, final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl(serverIndex, "/api/v1/server")))
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
