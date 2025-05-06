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
package com.arcadedb.server.ha;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

public class HTTPGraphConcurrentIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  public void testOneEdgePerTxMultiThreads() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript",
          "create vertex type Photos" + serverIndex + ";create vertex type Users" + serverIndex + ";create edge type HasUploaded"
              + serverIndex + ";");

      Thread.sleep(500);

      executeCommand(serverIndex, "sql", "create vertex Users" + serverIndex + " set id = 'u1111'");

      Thread.sleep(500);

      final int THREADS = 4;
      final int SCRIPTS = 100;
      final AtomicInteger atomic = new AtomicInteger();

      final Thread[] threads = new Thread[THREADS];
      for (int i = 0; i < THREADS; i++) {
        threads[i] = new Thread(() -> {
          for (int j = 0; j < SCRIPTS; j++) {
            try {
              final JSONObject responseAsJson = executeCommand(serverIndex, "sqlscript", //
                  "BEGIN ISOLATION REPEATABLE_READ;" //
                      + "LET photo = CREATE vertex Photos" + serverIndex + " SET id = uuid(), name = \"downloadX.jpg\";" //
                      + "LET user = SELECT FROM Users" + serverIndex + " WHERE id = \"u1111\";" //
                      + "LET userEdge = Create edge HasUploaded" + serverIndex + " FROM $user to $photo set type = \"User_Photos\";"
                      //
                      + "commit retry 100;return $photo;");

              atomic.incrementAndGet();

              if (responseAsJson == null) {
                LogManager.instance().log(this, Level.SEVERE, "Error on execution from thread %d", Thread.currentThread().threadId());
                continue;
              }

              assertThat(responseAsJson.getJSONObject("result").getJSONArray("records")).isNotNull();

            } catch (Exception e) {
              fail(e);
            }
          }
        });
        threads[i].start();
      }

      for (int i = 0; i < THREADS; i++)
        threads[i].join(60 * 1_000);

      assertThat(atomic.get()).isEqualTo(THREADS * SCRIPTS);

      final JSONObject responseAsJsonSelect = executeCommand(serverIndex, "sql", //
          "SELECT id FROM ( SELECT expand( outE('HasUploaded" + serverIndex + "') ) FROM Users" + serverIndex
              + " WHERE id = \"u1111\" )");

      assertThat(responseAsJsonSelect.getJSONObject("result").getJSONArray("records").length())
          .isEqualTo(THREADS * SCRIPTS)
          .withFailMessage("Some edges was missing when executing from server " + serverIndex);
    });
  }
}
