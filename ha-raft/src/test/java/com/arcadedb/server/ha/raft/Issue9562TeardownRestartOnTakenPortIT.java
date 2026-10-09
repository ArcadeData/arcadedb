/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9562: the teardown restarts a server the test left stopped and then compares every
 * database. The restart was pinned to the single HTTP port the server had bound before it stopped, so when another
 * process took that port while the server was down (a parallel build on the same machine, which is how
 * {@code Issue8900ClosedServerInPlaceRestartIT} failed), the restart threw, the server stayed down, and the comparison
 * read its copy from disk - which misses what the cluster wrote after it stopped - and reported
 * {@code DatabaseAreNotIdentical}, hiding the restart failure.
 * <p>
 * This test is the stranger itself: it stops a follower, writes a new type through the other two, takes the follower's
 * old HTTP port, and holds it through the teardown. The teardown must restart the follower on another port of its range,
 * let it catch up, and find the three databases identical.
 */
class Issue9562TeardownRestartOnTakenPortIT extends BaseRaftHATest {

  private ServerSocket stranger;
  private int          victim          = -1;
  private int          oldHttpPort     = -1;
  private int          restartedOnPort = -1;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Test
  void aStoppedServerWhoseHttpPortWasTakenIsRestartedOnAnotherPortAndCompared() throws IOException {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    victim = (leader + 1) % getServerCount();
    oldHttpPort = getServerHttpPort(victim);

    LogManager.instance().log(this, Level.INFO, "TEST: stopping server %d (HTTP port %d)", victim, oldHttpPort);
    getServer(victim).stop();

    // Written while the victim is down: a comparison against its on-disk copy cannot match.
    final Database db = getServerDatabase(leader, getDatabaseName());
    db.transaction(() -> db.getSchema().createVertexType("AfterStop"));
    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newVertex("AfterStop").set("id", i).save();
    });

    // Another process takes the port the victim released, on the address the fixture binds.
    stranger = new ServerSocket();
    stranger.setReuseAddress(true);
    stranger.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), oldHttpPort));
  }

  @Override
  protected void startServer(final int serverIndex) {
    super.startServer(serverIndex);
    if (serverIndex == victim)
      restartedOnPort = getServerHttpPort(serverIndex);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      super.endTest();
    } finally {
      try {
        if (stranger != null)
          stranger.close();
      } catch (final IOException e) {
        // the port is released when the JVM exits
      }
    }

    // super.endTest() ran the comparison and threw if it failed; it ran against the restarted victim, not its old copy.
    assertThat(restartedOnPort).as("the teardown must have restarted server %d", victim).isPositive();
    assertThat(restartedOnPort).as("the teardown must have moved past the taken port %d", oldHttpPort).isNotEqualTo(oldHttpPort);
  }
}
