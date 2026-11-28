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

import com.arcadedb.server.HAServerPlugin;
import org.xnio.XnioIoThread;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An HA plugin reduced to the one call the authentication of a cluster-issued session token makes: asking the node
 * that issued a token whether it still holds the session. The test scripts the answer and reads back which thread
 * each question was asked on, which is what issue #9864 is about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ScriptedAuthSessionPeer implements HAServerPlugin {
  /** One question put to the issuer: the token, the thread that asked, and whether that is an Undertow IO thread. */
  public record Lookup(String issuer, String token, String thread, boolean onIoThread) {
  }

  private final    List<Lookup>    lookups = new CopyOnWriteArrayList<>();
  private volatile PeerAuthSession answer;
  private volatile Runnable        whenAsked;

  /** The issuer vouches for the session of {@code userName} from now on. */
  public ScriptedAuthSessionPeer vouchesFor(final String userName) {
    answer = new PeerAuthSession(userName, System.currentTimeMillis());
    return this;
  }

  /** The issuer does not know the token from now on. */
  public ScriptedAuthSessionPeer refuses() {
    answer = null;
    return this;
  }

  /**
   * Runs {@code action} each time the issuer is asked, after the answer was taken and before it is given back: whatever it
   * changes happens while the question is in flight, so the answer does not reflect it. {@code null} removes it.
   */
  public ScriptedAuthSessionPeer whenAsked(final Runnable action) {
    whenAsked = action;
    return this;
  }

  public List<Lookup> lookups() {
    return lookups;
  }

  @Override
  public PeerAuthSession lookupAuthSession(final String issuerServerName, final String token) throws IOException {
    final Thread thread = Thread.currentThread();
    lookups.add(new Lookup(issuerServerName, token, thread.getName(), thread instanceof XnioIoThread));
    final PeerAuthSession given = answer;
    final Runnable action = whenAsked;
    if (action != null)
      action.run();
    return given;
  }

  @Override
  public boolean isLeader() {
    return true;
  }

  @Override
  public String getLeaderName() {
    return null;
  }

  @Override
  public ELECTION_STATUS getElectionStatus() {
    return ELECTION_STATUS.DONE;
  }

  @Override
  public void startService() {
  }

  @Override
  public String getClusterName() {
    return "issue9864";
  }

  @Override
  public Map<String, Object> getStats() {
    return Collections.emptyMap();
  }

  @Override
  public int getConfiguredServers() {
    return 2;
  }

  @Override
  public String getLeaderAddress() {
    return null;
  }

  @Override
  public String getReplicaAddresses() {
    return "";
  }

  @Override
  public void shutdownRemoteServer(final String serverName) {
  }

  @Override
  public void disconnectCluster() {
  }
}
