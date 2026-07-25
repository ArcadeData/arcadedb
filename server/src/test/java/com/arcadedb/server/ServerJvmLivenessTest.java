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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5418 counterpart: the engine's background threads became daemons so that a leaked embedded
 * {@code Database} cannot keep a host JVM alive. {@code ArcadeDBServer.main()} returns right after
 * {@code start()}, so the standalone server process stays up ONLY because of the non-daemon threads its HTTP
 * listener owns. This test pins that invariant down: should the HTTP stack ever be configured with daemon
 * threads, {@code arcadedb server} would exit silently a few milliseconds after boot.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerJvmLivenessTest extends BaseGraphServerTest {

  @Test
  public void httpListenerKeepsTheServerProcessAlive() {
    final List<String> nonDaemonHttpThreads = new ArrayList<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.isAlive() && !t.isDaemon() && t.getName().startsWith("XNIO-"))
        nonDaemonHttpThreads.add(t.getName());

    assertThat(nonDaemonHttpThreads)
        .as("The running server must own at least one non-daemon thread, or its JVM would exit as soon as main() returns (issue #5418)")
        .isNotEmpty();
  }
}
