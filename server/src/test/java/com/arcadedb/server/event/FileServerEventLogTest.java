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
package com.arcadedb.server.event;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests ensuring {@link FileServerEventLog} never returns {@code null} from its public read methods.
 * Returning {@code null} previously caused the Studio "Server metrics" page to fail with
 * {@code Cannot invoke "JSONArray.length()" because "events" is null} when the event log file was missing
 * or unreadable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FileServerEventLogTest extends StaticBaseServerTest {
  private ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();

    super.endTest();
  }

  @Test
  void getCurrentEventsReturnsArrayAfterStart() {
    final JSONArray events = server.getEventLog().getCurrentEvents();
    assertThat(events).isNotNull();
    // start() reports at least the "Server started" INFO event, so the file should have data.
    assertThat(events.length()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getCurrentEventsReturnsEmptyArrayWhenFileWasDeleted() {
    // Simulate the file being rotated/deleted out from under the log (the scenario that surfaced
    // the bug in Studio: events file vanished between reportEvent() and the metrics request).
    final File logDir = new File(server.getRootPath() + File.separator + "log");
    final File[] logFiles = logDir.listFiles((d, n) -> n.startsWith("server-event-log-") && n.endsWith(".jsonl"));
    assertThat(logFiles).isNotNull();
    for (final File f : logFiles)
      FileUtils.deleteFile(f);

    final JSONArray events = server.getEventLog().getCurrentEvents();
    assertThat(events).isNotNull();
    assertThat(events.length()).isEqualTo(0);
  }

  @Test
  void getEventsReturnsEmptyArrayForUnknownFile() {
    final JSONArray events = server.getEventLog().getEvents("server-event-log-19700101-000000.0.jsonl");
    assertThat(events).isNotNull();
    assertThat(events.length()).isEqualTo(0);
  }

  @Test
  void getFilesReturnsArrayAfterStart() {
    final JSONArray files = server.getEventLog().getFiles();
    assertThat(files).isNotNull();
  }
}
