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

import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10769: a membership REMOVAL (and the leave that leads to one) built its compare-and-set precondition, its
 * "is this peer a member" check and its quorum guard from {@link RaftHAServer#getLivePeers()}, which substitutes the
 * declared server list while the division cannot be read. The sibling ADD path had been moved onto the committed
 * configuration for exactly that reason. Every membership change now receives a
 * {@link RaftClusterManager.CommittedConfiguration} and nothing else, and refuses with the reason when it cannot be read.
 * <p>
 * The cluster view is a {@link FakeRaftHAServer} whose declared list (what {@code getLivePeers()} answers) and committed
 * configuration differ on purpose; the Raft client is a {@link Proxy} that records what would have been sent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10769MembershipChangeReadsCommittedConfigurationTest {
  private static final RaftPeerId SELF = RaftPeerId.valueOf("A");

  private final    List<SetConfigurationRequest.Arguments> sent = new CopyOnWriteArrayList<>();
  private          FakeRaftHAServer                        server;
  // The failure the recording client answers with; null answers every change as committed
  private volatile IOException                             failWith;

  @BeforeEach
  void setUp() {
    server = FakeRaftHAServer.detached();
    server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));
    server.localPeerId(SELF);
    server.client(recordingClient());
    // The declared list: longer than the membership, as when the division is unreadable or a declared peer never joined
    server.livePeers(List.of(peer("A"), peer("B"), peer("C"), peer("D"), peer("E")));
  }

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void aRemovalSendsTheCommittedConfigurationAsItsPrecondition() {
    server.committedPeers(List.of(peer("A"), peer("B"), peer("C")));

    new RaftClusterManager(server).removePeer("C");

    assertThat(sent).hasSize(1);
    assertThat(sent.getFirst().getServersInCurrentConf()).extracting(p -> p.getId().toString())
        .as("the precondition the leader compares against").containsExactly("A", "B", "C");
    assertThat(sent.getFirst().getServersInNewConf()).extracting(p -> p.getId().toString()).containsExactly("A", "B");
  }

  @Test
  void aRemovalRefusesWhenTheCommittedConfigurationCannotBeRead() {
    server.committedPeers(null);

    // Cuts the bounded wait for a configuration that cannot be read short; the refusal is the same after it
    Thread.currentThread().interrupt();
    assertThatThrownBy(() -> new RaftClusterManager(server).removePeer("C"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to remove peer C")
        .hasMessageContaining("cannot read the live Raft configuration");
    assertThat(Thread.currentThread().isInterrupted()).as("the interrupt is preserved").isTrue();
    assertThat(sent).as("nothing the leader could never match is sent").isEmpty();
  }

  /**
   * The refusal ends the change, it is not retried until the budget runs out: the configuration becomes readable right
   * after the bounded wait, and nothing is sent anyway. Counted in reads, not in time.
   */
  @Test
  void anUnreadableConfigurationEndsTheChangeInsteadOfBeingRetried() {
    final AtomicInteger reads = new AtomicInteger();
    // One read, then the 50 of the bounded wait, all unreadable; any read after that would find the configuration
    server.on("getCommittedPeersOrNull", args -> reads.incrementAndGet() <= 51 ? null : List.of(peer("A"), peer("B")));

    assertThatThrownBy(() -> new RaftClusterManager(server).addPeer("F", "h6:2434"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("cannot read the live Raft configuration");
    assertThat(reads.get()).as("one bounded wait, then the refusal").isEqualTo(51);
    assertThat(sent).isEmpty();
  }

  @Test
  void aDeclaredPeerThatIsNotAMemberIsNotFound() {
    server.committedPeers(List.of(peer("A"), peer("B"), peer("C")));

    assertThatThrownBy(() -> new RaftClusterManager(server).removePeer("E"))
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("Peer E not found in cluster");
    assertThat(sent).isEmpty();
  }

  @Test
  void theQuorumGuardCountsTheMembersNotTheDeclaredList() {
    // Two members: removing one leaves one voter, below the quorum of two. Counted over the five declared peers it
    // would have passed (four left, quorum three).
    server.committedPeers(List.of(peer("A"), peer("B")));

    assertThatThrownBy(() -> new RaftClusterManager(server).removePeer("B"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Refusing to remove peer B")
        .hasMessageContaining("current 2-node configuration");
    assertThat(sent).isEmpty();
  }

  @Test
  void aLeaveRefusesWhenTheCommittedConfigurationCannotBeRead() {
    server.committedPeers(null);

    Thread.currentThread().interrupt();
    assertThatThrownBy(() -> new RaftClusterManager(server).leaveCluster(false))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to leave the cluster")
        .hasMessageContaining("cannot read the live Raft configuration");
    assertThat(sent).isEmpty();
  }

  @Test
  void aLeaveOfTheOnlyMemberIsSkippedWhateverTheDeclaredListSays() {
    server.committedPeers(List.of(peer("A")));

    new RaftClusterManager(server).leaveCluster(false);

    assertThat(sent).as("a single-node cluster has nobody to leave").isEmpty();
  }

  @Test
  void anAddRefusesWhenTheCommittedConfigurationCannotBeRead() {
    server.committedPeers(null);

    Thread.currentThread().interrupt();
    assertThatThrownBy(() -> new RaftClusterManager(server).addPeer("F", "h6:2434"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to add peer F")
        .hasMessageContaining("cannot read the live Raft configuration");
    assertThat(sent).isEmpty();
  }

  /**
   * An add that failed, checked while the configuration is momentarily unreadable: the check waits for a readable
   * configuration instead of deciding on the window, so a change that committed after all keeps its declared address.
   */
  @Test
  void aFailedAddCheckedDuringARestartWindowKeepsTheAddressOfAPeerThatCommitted() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    server.httpAddresses(httpAddresses);
    final AtomicInteger reads = new AtomicInteger();
    // The attempt reads {A,B,C}; the failure check then meets the restart window once, then the committed {A,B,C,F}
    server.on("getCommittedPeersOrNull", args -> switch (reads.incrementAndGet()) {
      case 1 -> List.of(peer("A"), peer("B"), peer("C"));
      case 2 -> null;
      default -> List.of(peer("A"), peer("B"), peer("C"), peer("F"));
    });
    failWith = new IOException("the reply was lost");

    final RaftPeer newPeer = RaftPeer.newBuilder().setId("F").setAddress("h-F:2434").build();
    assertThatThrownBy(() -> new RaftClusterManager(server, 0).addPeer(newPeer, null, "h-F:2480"))
        .isInstanceOf(ConfigurationException.class);

    assertThat(reads.get()).as("the unreadable read was waited out, not believed").isGreaterThanOrEqualTo(3);
    assertThat(httpAddresses).containsEntry(RaftPeerId.valueOf("F"), "h-F:2480");
  }

  /** The other side of the same check: a readable configuration without the peer withdraws the declared address. */
  @Test
  void aFailedAddOfAPeerThatDidNotCommitWithdrawsTheAddress() {
    final Map<RaftPeerId, String> httpAddresses = new ConcurrentHashMap<>();
    server.httpAddresses(httpAddresses);
    server.committedPeers(List.of(peer("A"), peer("B"), peer("C")));
    failWith = new IOException("refused");

    final RaftPeer newPeer = RaftPeer.newBuilder().setId("F").setAddress("h-F:2434").build();
    assertThatThrownBy(() -> new RaftClusterManager(server, 0).addPeer(newPeer, null, "h-F:2480"))
        .isInstanceOf(ConfigurationException.class);

    assertThat(httpAddresses).doesNotContainKey(RaftPeerId.valueOf("F"));
  }

  /**
   * The structural half of the guarantee: the retry loop takes a change built FROM a committed configuration, so no
   * overload accepting a free-standing supplier of arguments (which could read any accessor) may exist.
   */
  @Test
  void everyRetryLoopTakesAChangeBuiltFromTheCommittedConfiguration() {
    final List<Method> loops = Stream.of(RaftClusterManager.class.getDeclaredMethods())
        .filter(m -> m.getName().equals("setConfigurationWithRetry")).toList();

    assertThat(loops).isNotEmpty();
    for (final Method loop : loops) {
      assertThat(loop.getParameterTypes()).as(loop.toString()).contains(RaftClusterManager.MembershipChange.class);
      assertThat(loop.getParameterTypes()).as(loop.toString()).doesNotContain(Supplier.class);
    }
    assertThat(Stream.of(RaftClusterManager.CommittedConfiguration.class.getDeclaredConstructors())
        .allMatch(c -> Modifier.isPrivate(c.getModifiers())))
        .as("only RaftClusterManager.readCommittedConfiguration builds one").isTrue();
  }

  /**
   * The census half, enumerated from the sources rather than from a hand-written list: every place in the module that
   * builds a compare-and-set precondition does it in a method handed a {@code CommittedConfiguration}.
   */
  @Test
  void everyPreconditionInTheModuleIsBuiltFromTheCommittedConfiguration() throws IOException {
    final Path sources = Path.of("src/main/java/com/arcadedb/server/ha/raft");
    assertThat(sources).isDirectory();

    // A method declaration: modifiers, a return type, a name, a parameter list, an optional throws clause, then a body
    final Pattern methodHeader = Pattern.compile("^\\s*(?:(?:private|static|final|public|protected|synchronized)\\s+)*"
        + "[A-Za-z][\\w.<>\\[\\], ?]*\\s+(\\w+)\\(([^)]*)\\)\\s*(?:throws\\s+[\\w., ]+)?\\{", Pattern.MULTILINE);
    final List<String> preconditionSites = new ArrayList<>();
    try (Stream<Path> files = Files.list(sources)) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        int at = source.indexOf(".setServersInCurrentConf(");
        while (at >= 0) {
          // The nearest method declaration above the call: its parameters must carry the committed configuration
          final Matcher matcher = methodHeader.matcher(source.substring(0, at));
          String method = null;
          String parameters = null;
          while (matcher.find()) {
            method = matcher.group(1);
            parameters = matcher.group(2);
          }
          preconditionSites.add(file.getFileName() + "#" + method);
          assertThat(parameters).as(file.getFileName() + "#" + method
              + " builds a compare-and-set precondition without being handed the committed configuration").contains(
              "CommittedConfiguration");
          at = source.indexOf(".setServersInCurrentConf(", at + 1);
        }
      }
    }
    assertThat(preconditionSites).as("the add and the removal")
        .contains("RaftClusterManager.java#buildAddArgs", "RaftClusterManager.java#buildRemoveArgs");
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("h-" + id + ":2434").build();
  }

  /** A Raft client whose admin API records each configuration change and answers it as committed. */
  private RaftClient recordingClient() {
    final AdminApi admin = new AdminApi() {
      @Override
      public RaftClientReply setConfiguration(final SetConfigurationRequest.Arguments arguments) throws IOException {
        sent.add(arguments);
        if (failWith != null)
          throw failWith;
        return RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(SELF)
            .setGroupId(RaftGroupId.randomId()).setCallId(sent.size()).setSuccess(true).build();
      }

      @Override
      public RaftClientReply transferLeadership(final RaftPeerId expectedLeader, final RaftPeerId newLeader,
          final long timeoutMs) {
        throw new UnsupportedOperationException("not part of a membership change");
      }
    };
    return (RaftClient) Proxy.newProxyInstance(RaftClient.class.getClassLoader(), new Class<?>[] { RaftClient.class },
        (proxy, method, args) -> switch (method.getName()) {
          case "admin" -> admin;
          case "close" -> null;
          case "toString" -> "recording RaftClient";
          case "hashCode" -> System.identityHashCode(proxy);
          case "equals" -> proxy == args[0];
          default -> throw new UnsupportedOperationException(method.getName());
        });
  }
}
