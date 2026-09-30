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
package com.arcadedb.server.monitor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7869, the server side: {@code GET /api/v1/server} must report the disk figures of the directory THIS
 * server's configuration names, and that directory must be the one the server's low-disk warning measures.
 * <p>
 * The fixture sets {@code arcadedb.server.databaseDirectory} to {@code ./target/databases} on the process-wide
 * enum and to {@code ./target/databases0} on the server's own {@code ContextConfiguration} - the same split a
 * {@code config/server-configuration.json} produces. Before the fix the profiler read the enum and reported the
 * former.
 */
class Issue7869ServerDiskDirectoryTest extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void theServerEndpointReportsTheDirectoryTheServerIsConfiguredWith() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final String configured = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    assertThat(configured).as("precondition: the server's overlay and the enum disagree")
        .isNotEqualTo(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString());

    final File expected = ServerMonitor.resolveDiskSpaceDirectory(server.getConfiguration()).getCanonicalFile();

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();
    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);

    final JSONObject profiler = new JSONObject(response.body()).getJSONObject("metrics").getJSONObject("profiler");
    final File reported = new File(profiler.getJSONObject("diskDirectory").getString("value")).getCanonicalFile();

    assertThat(reported).as("the directory the low-disk warning measures").isEqualTo(expected);
    assertThat(reported).isEqualTo(new File(configured).getCanonicalFile());
  }

  @Test
  void aStoppedServerIsNoLongerTheOneReported() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final File configured = new File(
        server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)).getCanonicalFile();
    assertThat(reportedDirectory()).isEqualTo(configured);

    server.stop();

    assertThat(reportedDirectory()).as("the stopped server's configuration must be withdrawn from the profiler")
        .isNotEqualTo(configured);
  }

  private static File reportedDirectory() throws IOException {
    return new File(Profiler.INSTANCE.toJSON().getJSONObject("diskDirectory").getString("value")).getCanonicalFile();
  }
}
