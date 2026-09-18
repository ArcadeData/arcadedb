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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.http.HttpServer;

import org.apache.ratis.protocol.RaftPeerId;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A multi-node Raft cluster whose HTTP layer really speaks TLS: every node binds an HTTPS listener with a
 * CA-signed certificate, every node trusts that CA, and {@code arcadedb.ha.serverList} declares each peer's
 * {@code https} port, so the peer-to-peer dials resolve an encrypted endpoint and take it (issue #7563).
 * <p>
 * <b>Why the fixture and not more unit tests.</b> Every peer-to-peer dial in this module chooses its scheme
 * through a small pure function ({@code LeaderDial.resolve}, {@link PeerCapabilityQuery#chooseUrl},
 * {@link LeaderDatabaseQuery#chooseEndpoint}, {@link BootstrapElection#chooseUrl},
 * {@link RaftHAPlugin#shutdownUrl}) and every one of those is unit-tested against a recording client. What no
 * recording client can reach is what only exists once a socket is opened: whether the certificate the peer
 * presents is issued for the name the dial asked for, whether the truststore the node is configured with is
 * the one being consulted, and whether a peer that is NOT signed by the cluster CA is actually rejected. A
 * test JVM usually has no truststore configured at all, which is exactly the state in which
 * {@code SnapshotInstaller.buildSSLContext} falls back to {@code SSLContext.getDefault()} - so a genuinely
 * misconfigured truststore and a correct one are indistinguishable to a test that never handshakes.
 * <p>
 * <b>Ports.</b> {@link BaseRaftHATest} lets the plain HTTP listener land on 2480+index by range scan and
 * patches the resolved addresses after startup. The HTTPS listener is pinned per node instead of ranged,
 * because the address a peer dials is declared in the server list before any node has bound anything: a
 * ranged port that drifted would be declared wrong and there would be nothing to patch it against. A port
 * already in use therefore fails the node's startup loudly rather than silently moving the listener
 * somewhere the cluster is not looking.
 * <p>
 * The PKI is generated per subclass into {@code target/}, never committed and never reused across runs; see
 * {@link RaftTestPki}. One certificate serves the whole cluster because it is issued for {@code CN=localhost}
 * with {@code SAN=dns:localhost,ip:127.0.0.1}, and every in-process node is dialled as {@code localhost}.
 */
public abstract class BaseRaftHASslTest extends BaseRaftHATest {

  /** Kept clear of 2480-2489 (plain HTTP) and of the JDK-default 2490-2499 range, to make a clash obvious. */
  private static final int BASE_HTTPS_PORT = 2590;

  /**
   * The certificate authority of each SSL suite that has run in this JVM, keyed by its
   * {@link #pkiDirectoryName()}.
   * <p>
   * Static and keyed rather than a plain instance field: JUnit 5's default lifecycle builds a fresh test
   * instance per {@code @Test} method, so an instance field would run {@code keytool} five times for a
   * five-method class, and a plain static field would hand the second suite in the JVM the first one's key
   * material - under the second suite's directory name, which is the confusing half. The key makes "once per
   * class" true rather than merely intended (claude-review on PR #7838).
   * <p>
   * {@code RaftTestPki.create} deletes the directory's files before regenerating them, so two suites sharing
   * one key would race; sharing by key is what stops that as well.
   */
  private static final Map<String, RaftTestPki> PKI_BY_DIRECTORY = new ConcurrentHashMap<>();

  /** The cluster's own certificate authority and the single node identity it signed, for THIS suite. */
  private RaftTestPki pki;

  /**
   * Names the {@code target/} directory this suite's key material is generated into, once per class. Every
   * subclass gives a name of its own so two SSL suites in one JVM cannot overwrite each other's.
   */
  protected abstract String pkiDirectoryName();

  /**
   * The subject alternative names the node certificate is issued for, in keytool's {@code -ext} syntax.
   * <p>
   * Overridable because a certificate that always matches the name dialled proves nothing about the hostname
   * check (issue #7836): a suite that issues for the WRONG name is what shows the check is performed, and one
   * that issues for SEVERAL is what shows a peer can serve more than one of them. The default covers
   * {@code localhost} and {@code 127.0.0.1}, which is how every in-process node is dialled, so a subclass that
   * overrides this is declaring that its cluster's peer-to-peer HTTP dials are expected to fail.
   */
  protected String subjectAltNames() {
    return RaftTestPki.SUBJECT_ALT_NAMES;
  }

  /** The cluster PKI, available to a test that wants to build a client of its own against the same CA. */
  protected RaftTestPki clusterPki() {
    return pki;
  }

  /** The HTTPS port node {@code index} is configured to listen on. */
  protected int httpsPortOf(final int index) {
    return BASE_HTTPS_PORT + index;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    if (pki == null)
      pki = PKI_BY_DIRECTORY.computeIfAbsent(pkiDirectoryName(), directory -> {
        try {
          return RaftTestPki.create(Path.of("target", directory), "cluster", subjectAltNames());
        } catch (final Exception e) {
          throw new IllegalStateException("Cannot generate the test PKI for the SSL cluster fixture", e);
        }
      });

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    config.setValue(GlobalConfiguration.SERVER_HTTPS_INCOMING_PORT, String.valueOf(httpsPortOf(index)));
    // The identity this node's HTTPS listener presents.
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, pki.nodeKeyStore().toAbsolutePath().toString());
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, RaftTestPki.password());
    // The anchors it validates a PEER against - read by HttpServer.createSSLContext for the listener and by
    // SnapshotInstaller.buildSSLContext for every outbound peer-to-peer dial. Configuring it is the whole
    // point: without it the dials fall back to SSLContext.getDefault() and prove nothing (issue #7563).
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, pki.trustStore().toAbsolutePath().toString());
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, RaftTestPki.password());
  }

  /**
   * The five-field form of every entry - {@code host:raftPort:httpPort:priority:httpsPort} - so each peer's
   * HTTPS endpoint is DECLARED rather than derived. The derive fallback would resolve every peer's HTTPS
   * endpoint to this node's own port, which on a cluster whose nodes differ by port is either this node
   * itself or the wrong peer; declaring it is what the fixture exists to exercise.
   */
  @Override
  protected String getServerAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:").append(raftPortOf(i)).append(":").append(2480 + i).append(":0:").append(httpsPortOf(i));
    }
    return sb.toString();
  }

  /**
   * Patches the HTTPS addresses with the ports each node actually bound, the encrypted twin of what
   * {@link BaseRaftHATest#startServers()} already does for the plain ones. The declared ports above are
   * normally exactly right; this closes the window in which they are not, and asserts loudly rather than
   * letting a node whose HTTPS listener never came up look like a cluster that simply prefers plain HTTP.
   */
  @Override
  protected void startServers() {
    super.startServers();

    for (int i = 0; i < getServerCount(); i++) {
      final HttpServer httpServer = getServer(i).getHttpServer();
      if (httpServer == null || httpServer.getHttpsPort() <= 0)
        throw new IllegalStateException(
            "Server " + i + " did not bind an HTTPS listener; the SSL cluster fixture cannot prove anything "
                + "about TLS peer dials without one");
    }

    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin == null || plugin.getRaftHAServer() == null)
        continue;
      final Map<RaftPeerId, String> httpsAddresses = plugin.getRaftHAServer().getHttpsAddresses();
      for (int j = 0; j < getServerCount(); j++)
        httpsAddresses.put(RaftPeerId.valueOf(peerIdForIndex(j)),
            "localhost:" + getServer(j).getHttpServer().getHttpsPort());
    }
  }

  /** The Raft port node {@code index} listens on, read back from the peer id {@link BaseRaftHATest} assigns. */
  protected int raftPortOf(final int index) {
    final String peerId = peerIdForIndex(index);
    return Integer.parseInt(peerId.substring(peerId.lastIndexOf('_') + 1));
  }
}
