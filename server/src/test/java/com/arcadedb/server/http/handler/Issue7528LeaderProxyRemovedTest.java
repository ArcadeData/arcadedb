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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.LeaderForwardContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #7528 (with #7547 and #7551, which report the same thing): {@code LeaderProxy} was a complete
 * follower-to-leader HTTP proxy - body capping, hop-by-hop header stripping, cluster-token injection, response
 * relay - that <b>nothing ever constructed</b>. Its Javadoc said it was "invoked from
 * {@code AbstractServerHttpHandler}"; no {@code new LeaderProxy} and no {@code tryProxy} call existed anywhere
 * under {@code src/main}. Task 9 of the Ratis port, which was to wire it in, never landed after Task 8 built it.
 * <p>
 * It was deleted rather than revived. Everything it was for already runs, in code that has the properties it
 * lacked: {@link LeaderCommandForwarder} relays the administrative routes, resolving the cluster token through
 * {@code HAServerPlugin.effectiveClusterToken} and setting the one-hop marker, while
 * {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft} carries SQL and DDL writes. {@code LeaderProxy}
 * read the RAW {@code arcadedb.ha.clusterToken} setting, which is blank on every cluster that did not declare one
 * explicitly, so had it ever been wired in it would have refused every request on a default-configured cluster.
 * <p>
 * What this test pins is the removal, because the failure it prevents is one a green suite cannot show: a class
 * nothing calls has no failing test, and a fix applied to it - #7508 fixed its {@code http://} scheme - is a
 * no-op that reads as done.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7528LeaderProxyRemovedTest {

  @Test
  void theUnwiredProxyIsNotOnTheClasspath() {
    assertThatCode(() -> Class.forName("com.arcadedb.server.http.handler.LeaderProxy"))
        .as("reviving LeaderProxy means revisiting issue #7528, not restoring the class as it was")
        .isInstanceOf(ClassNotFoundException.class);
  }

  /**
   * {@code arcadedb.ha.proxyMaxBodySize} went with it: the proxy's constructor was its only reader, so the
   * setting was documented, tunable and inert. Its two siblings stay, because they DO have live readers -
   * {@link LeaderCommandForwarder}, {@code RaftReplicatedDatabase}, {@code PostBatchHandler} and
   * {@code TrustedHttpClientCache}.
   */
  @Test
  void theSettingThatOnlyTheProxyReadIsGoneAndItsLiveSiblingsAreNot() {
    assertThat(GlobalConfiguration.findByKey("arcadedb.ha.proxyMaxBodySize")).isNull();

    assertThat(GlobalConfiguration.findByKey("arcadedb.ha.proxyReadTimeout"))
        .isEqualTo(GlobalConfiguration.HA_PROXY_READ_TIMEOUT);
    assertThat(GlobalConfiguration.findByKey("arcadedb.ha.proxyConnectTimeout"))
        .isEqualTo(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT);
  }

  /**
   * Removing a declared setting must not break a server whose configuration file still names it. An unknown key
   * is dropped with a warning ({@code ContextConfiguration.fromJSON}), so an operator upgrading with the old line
   * in place starts normally instead of failing at parse time.
   */
  @Test
  void aConfigurationFileStillNamingTheRemovedSettingLoads() {
    final ContextConfiguration configuration = new ContextConfiguration();

    assertThatCode(() -> configuration.fromJSON(
        "{\"configuration\":{\"ha.proxyMaxBodySize\":1048576,\"ha.proxyReadTimeout\":12345}}"))
        .doesNotThrowAnyException();

    assertThat(configuration.getValueAsLong(GlobalConfiguration.HA_PROXY_READ_TIMEOUT))
        .as("the surviving settings in the same file are still applied").isEqualTo(12345L);
  }

  /**
   * The Javadoc of {@link com.arcadedb.server.LeaderForwardContext} used to name the proxy as one of the places
   * the one-hop rule is enforced, which is the sentence a reader of that design consults. The marker itself is
   * what the surviving paths set, so assert it is still the contract they share.
   */
  @Test
  void theOneHopMarkerIsUnchanged() {
    assertThat(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER).isEqualTo("X-ArcadeDB-Forwarded-To-Leader");
  }
}
