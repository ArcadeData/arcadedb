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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10068: issue #9999 moved every peer-to-peer dial of {@code ha-raft} onto one builder, {@link PeerRequest}, and guarded it
 * with a source scan of that module. Three dials of THIS module spelled the same credential pair by hand, outside that scan:
 * {@code LeaderCommandForwarder} (the follower's command forward), {@code PostBatchHandler} (the batch forward) and
 * {@code SupportPeerQuery}. The builder now lives here, which is the module {@code ha-raft} depends on, and this is the scan of this
 * module's main sources: no cluster header is spelled, and no request is built, anywhere but in the builder.
 * <p>
 * The one shape that does not carry the pair - the forward that relays the client's own Basic or API-token credential (issue #7516) -
 * is {@link PeerRequest#newRelayRequest}, a method of the builder, so it needs no exemption here.
 * <p>
 * It is a tripwire, not a proof: a text scan cannot see a header name assembled at runtime or a call split across lines. It catches
 * the shape every dial in this module has actually taken, which is the shape the next one will copy.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10068ServerPeerDialsGoThroughOneBuilderTest {

  private static final Path   MAIN_SOURCES = Path.of("src", "main", "java");
  private static final String BUILDER      = "PeerRequest.java";

  /** Every shape a peer dial in this module could take to bypass the builder. */
  private static final String[] BYPASSES = {
      "\"X-ArcadeDB-Cluster-Token\"",   // the token, spelled by hand
      "\"X-ArcadeDB-Forwarded-User\"",  // the principal, spelled by hand
      "HttpRequest.newBuilder(",        // a request built without the credential and the notice
      ".openConnection()",              // the same, in the HttpURLConnection form
      "toURL()",                        // any URL-level dial (openConnection, openStream) starts from one
  };

  /**
   * The files that may build a request or a URL of their own, and the bypass each may use. None of them is a peer dial, which is the
   * whole reason they are listed: they talk to services OUTSIDE the cluster (the support portal, an AI provider) or open a file, and
   * must never carry the cluster token, which the two header rules above keep true of them as of every other file. A new file that
   * dials a peer is not on this list, and fails the scan.
   */
  private static final Map<String, Set<String>> OUTSIDE_THE_CLUSTER = Map.of(
      "SupportConnector.java", Set.of("HttpRequest.newBuilder("),
      "SupportPortalClient.java", Set.of("HttpRequest.newBuilder("),
      "AiAnalyzeProfilerHandler.java", Set.of("HttpRequest.newBuilder("),
      "AiActivateHandler.java", Set.of("HttpRequest.newBuilder("),
      "AiChatHandler.java", Set.of("HttpRequest.newBuilder("),
      "PluginClassLoader.java", Set.of("toURL()")); // the URL of a plugin jar on disk

  @Test
  void noPeerDialBuildsItsRequestOrSpellsItsCredentialOutsideTheBuilder() throws IOException {
    assertThat(MAIN_SOURCES).as("the scan runs from the server module directory").isDirectory();

    final List<String> offenders = new ArrayList<>();
    final int[] builderHits = new int[BYPASSES.length];
    int scanned = 0;
    try (final Stream<Path> files = Files.walk(MAIN_SOURCES)) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
        scanned++;
        final String name = file.getFileName().toString();
        final boolean isBuilder = name.equals(BUILDER);
        final Set<String> allowed = OUTSIDE_THE_CLUSTER.getOrDefault(name, Set.of());
        final List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++)
          for (int b = 0; b < BYPASSES.length; b++)
            if (lines.get(i).contains(BYPASSES[b])) {
              if (isBuilder)
                builderHits[b]++;
              else if (!allowed.contains(BYPASSES[b]))
                offenders.add(name + ":" + (i + 1) + "  " + lines.get(i).strip());
            }
      }
    }

    assertThat(scanned).as("the scan reached this module's sources, so an empty offender list means something").isGreaterThan(200);
    for (int b = 0; b < BYPASSES.length; b++)
      assertThat(builderHits[b]).as("the builder itself is where '%s' lives - if not, the scan is looking for the wrong text",
          BYPASSES[b]).isPositive();
    assertThat(offenders)
        .as("every peer-to-peer dial of this module starts its request from PeerRequest, which attaches the cluster token and the "
            + "forwarded user together and says the plain-HTTP fallback notice (issues #9999, #10068)")
        .isEmpty();
  }

  /** The list above is about the files that exist: an entry for a file that is gone would exempt a future file of that name. */
  @Test
  void everyExemptFileStillExistsAndStillNeedsItsExemption() throws IOException {
    final List<String> stale = new ArrayList<>();
    try (final Stream<Path> files = Files.walk(MAIN_SOURCES)) {
      final List<Path> all = files.filter(f -> f.toString().endsWith(".java")).toList();
      for (final Map.Entry<String, Set<String>> exempt : OUTSIDE_THE_CLUSTER.entrySet()) {
        final List<Path> matches = all.stream().filter(f -> f.getFileName().toString().equals(exempt.getKey())).toList();
        if (matches.size() != 1) {
          stale.add(exempt.getKey() + " matches " + matches.size() + " files");
          continue;
        }
        final String source = Files.readString(matches.getFirst(), StandardCharsets.UTF_8);
        for (final String bypass : exempt.getValue())
          if (!source.contains(bypass))
            stale.add(exempt.getKey() + " no longer uses " + bypass);
      }
    }
    assertThat(stale).as("an exemption nothing needs any more is a hole in the guard").isEmpty();
  }
}
