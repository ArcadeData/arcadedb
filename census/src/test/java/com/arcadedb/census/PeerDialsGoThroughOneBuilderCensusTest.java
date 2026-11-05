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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9999 and #10068: two helpers extracted to make the peer-to-peer dials spell their credential and their plain-HTTP
 * fallback notice in one place had 2 and 5 adopters out of 11 in {@code ha-raft}, and three dials of {@code server} spelled the
 * same credential pair by hand: a snapshot download sent the token with no principal, a probe spelled the root principal as a
 * literal, seven dials fell back to plain HTTP on an SSL cluster without a word. The helpers are now one builder,
 * {@code PeerRequest} (server module), which attaches the cluster token and the forwarded user together and says the notice; the
 * dials are pinned by {@code Issue9999PeerDialsGoThroughOneBuilderTest}.
 * <p>
 * The census ranges over the authority (#11288), in the compiled classes ({@link CompiledClasses}):
 * <ul>
 *   <li>every call that STARTS a request - {@code HttpRequest.newBuilder}, {@code URL.openConnection}, {@code URI.toURL} - in
 *       the two modules that dial peers, outside the builder. The ones that talk to services outside the cluster (the support
 *       portal, an AI provider, a jar on disk) are listed, and must never carry the cluster token, which the rule below keeps
 *       true of them;</li>
 *   <li>every call that SETS a header - {@code HttpRequest.Builder.header/setHeader},
 *       {@code URLConnection.setRequestProperty/addRequestProperty} - naming one of the two cluster headers, in every module,
 *       outside the builder ({@link CallOperands}).</li>
 * </ul>
 * The source-text censuses it replaces matched five spellings line by line, so a call split after the dot passed them, and they
 * could not tell the header named by a literal from the same header named through {@code PeerRequest.CLUSTER_TOKEN_HEADER},
 * which javac inlines: what the census reads now is the header a call attaches, however it is spelled.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PeerDialsGoThroughOneBuilderCensusTest {
  private static final List<String> DIALING_MODULES = List.of("server", "ha-raft");
  private static final String       BUILDER         = "server/PeerRequest";
  private static final Set<String>  CLUSTER_HEADERS = Set.of("X-ArcadeDB-Cluster-Token", "X-ArcadeDB-Forwarded-User");

  /** What starts a request: owner to method names. */
  private static final Map<String, Set<String>> STARTS_A_REQUEST = Map.of(//
      "java/net/http/HttpRequest", Set.of("newBuilder"),//
      "java/net/URL", Set.of("openConnection", "openStream"),//
      "java/net/URI", Set.of("toURL"));

  /** What attaches a header: owner to method names. */
  private static final Map<String, Set<String>> SETS_A_HEADER = Map.of(//
      "java/net/http/HttpRequest$Builder", Set.of("header", "setHeader"),//
      "java/net/URLConnection", Set.of("setRequestProperty", "addRequestProperty"),//
      "java/net/HttpURLConnection", Set.of("setRequestProperty", "addRequestProperty"),//
      "javax/net/ssl/HttpsURLConnection", Set.of("setRequestProperty", "addRequestProperty"));

  private static final String AI_PROVIDER    = "the AI gateway, a service outside the cluster";
  private static final String SUPPORT_PORTAL = "the support portal, a service outside the cluster";

  /**
   * The methods that start a request of their own, none of them a peer dial: they talk to services OUTSIDE the cluster, or open a
   * file. A new method that dials a peer is not on this list, and fails the census.
   */
  private static final Map<String, String> OUTSIDE_THE_CLUSTER = new TreeMap<>(Map.ofEntries(//
      Map.entry("server/ai/AiActivateHandler#execute", AI_PROVIDER),//
      Map.entry("server/ai/AiAnalyzeProfilerHandler#callGateway", AI_PROVIDER),//
      Map.entry("server/ai/AiChatHandler#callGateway", AI_PROVIDER),//
      Map.entry("server/ai/AiChatHandler#handleStreamingRequest", AI_PROVIDER),//
      Map.entry("server/ai/AiChatHandler#postToolResult", AI_PROVIDER),//
      Map.entry("server/plugin/PluginClassLoader#<init>", "the URL of a plugin jar on disk"),//
      Map.entry("server/support/SupportConnector#rawPost", SUPPORT_PORTAL),//
      Map.entry("server/support/SupportPortalClient#execute", SUPPORT_PORTAL),//
      Map.entry("server/support/SupportPortalClient#openStream", SUPPORT_PORTAL)));

  @Test
  void noPeerDialStartsItsRequestOutsideTheBuilder() throws Exception {
    final Map<String, byte[]> classes = new HashMap<>();
    for (final String module : DIALING_MODULES) {
      assertThat(CompiledClasses.byModule()).as("the census must read the " + module + " module").containsKey(module);
      classes.putAll(CompiledClasses.byModule().get(module));
    }

    final Map<String, Set<String>> callSites = CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> STARTS_A_REQUEST.getOrDefault(owner, Set.of()).contains(name));
    assertThat(callSites.keySet().stream().anyMatch(k -> k.startsWith(BUILDER + "#"))).as(
        "the census must reach the builder, where every peer request starts").isTrue();
    callSites.keySet().removeIf(k -> k.startsWith(BUILDER + "#"));

    assertThat(CompiledClasses.unreviewed(callSites, OUTSIDE_THE_CLUSTER)).as(
        "every peer-to-peer dial starts its request from PeerRequest.newRequest/openConnection, which attaches the cluster token "
            + "and forwarded user together and says the plain-HTTP fallback notice (issues #9999, #10068); a request to a service "
            + "outside the cluster goes in OUTSIDE_THE_CLUSTER with what it talks to").isEmpty();
    assertThat(CompiledClasses.stale(callSites, OUTSIDE_THE_CLUSTER)).as("entries that no longer start a request: remove them")
        .isEmpty();
  }

  @Test
  void noClassAttachesAClusterHeaderOutsideTheBuilder() throws Exception {
    final Set<String> attaching = new TreeSet<>();
    for (final CallOperands.Site site : CallOperands.of(CompiledClasses.ofEveryModule(),
        (owner, name, desc) -> SETS_A_HEADER.getOrDefault(owner, Set.of()).contains(name))) {
      final String header = CallOperands.stringConstant(site.argument(0));
      if (header != null && CLUSTER_HEADERS.contains(header))
        attaching.add(site.key());
    }

    assertThat(attaching).as("the census must reach the builder, where the cluster headers are attached").anyMatch(
        k -> k.startsWith(BUILDER + "#"));
    attaching.removeIf(k -> k.startsWith(BUILDER + "#"));
    assertThat(attaching).as("the cluster token and the forwarded user are attached by PeerRequest alone, together, or a dial "
        + "sends one without the other (issues #9999, #10068)").isEmpty();
  }
}
