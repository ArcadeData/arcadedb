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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11203: {@code raftGroup.getPeers()} is the server list as declared at startup, never the membership, and twice a fix to
 * what "replica" means was incomplete because a reporter kept reading it (#7040). The rule of {@code ha-raft/CLAUDE.md} used to be an
 * enumeration of the legitimate direct readers in prose, which went stale: it named three and the module had a dozen, each justified
 * at its own site. The enumeration is held here instead, by method, with the reason the method wants the declared list: a new
 * direct reader fails this test until it either reads the reconciled membership ({@code configuredPeers()}, {@code knownPeers()})
 * or is added below with its reason, and a reader that goes away takes its entry with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftGroupDeclaredPeerReadersTest {
  /** This module's production sources, seen from the {@code ha-raft} module that surefire runs this class in. */
  private static final Path SOURCES = Path.of("src", "main", "java");

  /** A read of this node's own declared group: the field, or the accessor that hands it out. */
  private static final Pattern DIRECT_READ = Pattern.compile("\\b(?:raftGroup|getRaftGroup\\(\\))\\s*\\.\\s*getPeers\\s*\\(\\s*\\)");

  /** A method or constructor declared as a class member (two-space indent), capturing its name. */
  private static final Pattern MEMBER_DECLARATION = Pattern.compile(
      "^  (?=\\S)(?:(?:public|protected|private|static|final|synchronized|abstract|default)\\s+)*(?:<[^>]+>\\s+)?(?:[\\w.<>\\[\\]?, ]+\\s+)?(\\w+)\\s*\\(");

  private static final Map<String, String> REVIEWED = Map.ofEntries(//
      Map.entry("RaftHAServer#getConfiguredServers", "the declared list, by name: how many servers the configuration declares"),
      Map.entry("RaftHAServer#getDeclaredPeers", "the declared list, by name: RaftClusterStatusExporter reconciles it itself"),
      Map.entry("RaftHAServer#configuredPeers", "an input of the reconciliation with the live configuration"),
      Map.entry("RaftHAServer#knownPeers", "an input of the reconciliation with the live configuration"),
      Map.entry("GetClusterHandler#execute", "an input of the reconciliation with the live configuration"),
      Map.entry("RaftHAServer#getLivePeers", "the fallback when the live configuration cannot be read"),
      Map.entry("RaftHAServer#peerRaftAddress", "the fallback after the live configuration: a declared peer it dropped still resolves"),
      Map.entry("RaftHAServer#isMemberWithoutReadingTheDivision", "the misdirected-request path must not read the division (#10088)"),
      Map.entry("RaftHAServer#isMisdirectedPeerMember", "the misdirected-request path must not read the division first (#10088)"),
      Map.entry("RaftHAServer#resolvePeerAddresses", "the misdirected-request path must not read the division (#10088)"),
      Map.entry("KubernetesAutoJoin#findLeader", "before joining, the declared peers are the only ones to probe for a leader"),
      Map.entry("KubernetesAutoJoin#joinFrom", "before joining, this node's own declared definition is what it asks to be added as"),
      Map.entry("RaftHAPlugin#resolveShutdownTarget", "an operator named a server: the answer is the peer the name declares"));

  @Test
  void everyDirectReaderOfTheDeclaredGroupIsReviewed() throws IOException {
    final Map<String, Integer> found = new TreeMap<>();
    try (final Stream<Path> files = Files.walk(SOURCES)) {
      for (final Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
        final String className = file.getFileName().toString().replace(".java", "");
        for (final String method : directReaders(Files.readString(file)))
          found.merge(className + "#" + method, 1, Integer::sum);
      }
    }

    assertThat(found).as("the scan found no reader at all, so it scanned nothing").isNotEmpty();
    assertThat(new TreeSet<>(found.keySet())).as(
            "direct readers of raftGroup.getPeers(), the DECLARED server list: read configuredPeers() or knownPeers() instead, or add the "
                + "method to REVIEWED with the reason it wants the declared list (ha-raft/CLAUDE.md); an entry whose reader is gone is removed")
        .containsExactlyInAnyOrderElementsOf(REVIEWED.keySet());
  }

  /** The scan itself, on a source it must read right, so that the test above cannot pass by finding nothing. */
  @Test
  void theScanNamesTheMethodEachReadIsIn() {
    final String source = """
        class Sample {
          private final RaftGroup raftGroup;

          public int count() {
            return Math.max(raftGroup.getPeers().size(), 0);
          }

          /** Mentions raftGroup.getPeers() in a comment line only. */
          static List<RaftPeer> twice(final RaftHAServer server, final int n) {
            final var a = server.getRaftGroup().getPeers();
            for (final RaftPeer p : raftGroup . getPeers ( ))
              use(p);
            return a;
          }

          private void unrelated() {
            reply.getGroup().getPeers();
            raftGroup.getGroupId();
          }
        }
        """;
    assertThat(directReaders(source)).containsExactly("count", "twice");
  }

  /** The members of {@code source} that read the declared group directly, each once. */
  private static TreeSet<String> directReaders(final String source) {
    final TreeSet<String> readers = new TreeSet<>();
    String member = null;
    for (final String line : source.split("\n")) {
      final String trimmed = line.trim();
      if (trimmed.startsWith("*") || trimmed.startsWith("/*") || trimmed.startsWith("//"))
        continue;
      final Matcher declaration = MEMBER_DECLARATION.matcher(line);
      if (declaration.find() && !isKeyword(declaration.group(1)))
        member = declaration.group(1);
      if (member != null && DIRECT_READ.matcher(line).find())
        readers.add(member);
    }
    return readers;
  }

  private static boolean isKeyword(final String name) {
    return switch (name) {
      case "if", "for", "while", "switch", "catch", "synchronized", "return", "new", "try" -> true;
      default -> false;
    };
  }
}
