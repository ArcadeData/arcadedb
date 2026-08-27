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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the shipped deployment examples honest against the code they launch. They are the first
 * thing an evaluator runs, they are never exercised by any other test, and every defect they grew
 * (issues #6840, #6841, #6842) was a silent divergence from a setting, a port or a variable that had
 * moved on: a {@code ${VAR}} Kubernetes never expands, a Raft port the server stopped binding, a
 * setting that no longer exists, a heap pinned above the container limit, a garbage collector
 * discarded by the quickstart that overwrites the variable holding it, and a named volume declared
 * but never mounted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PackagingManifestsTest {

  private static final String STATEFULSET = "package/src/main/config/arcadedb-statefulset.yaml";
  private static final String DOCKERFILE  = "package/src/main/docker/Dockerfile";
  private static final String SERVER_SH   = "package/src/main/scripts/server.sh";
  private static final String SERVER_BAT  = "package/src/main/scripts/server.bat";
  private static final String COMPOSE     = "docker-compose.yml";
  private static final String README      = "README.md";

  private static final Pattern SETTING     = Pattern.compile("-D(arcadedb\\.[A-Za-z0-9.]+)=");
  private static final Pattern EXPANSION   = Pattern.compile("\\$\\(([A-Za-z_][A-Za-z0-9_]*)\\)");
  private static final Pattern ENTRY_NAME  = Pattern.compile("^\\s*- name: (\\S+)\\s*$");
  private static final Pattern MOUNT_PATH  = Pattern.compile("^\\s*mountPath: (\\S+)\\s*$", Pattern.MULTILINE);

  @Test
  void statefulSetUsesOnlyTheVariableFormKubernetesExpands() {
    final String manifest = read(STATEFULSET);

    // ${VAR} is expanded neither by Kubernetes (which knows only $(VAR)) nor by a shell (the
    // arguments are exec'd directly), so it would reach the JVM as a literal.
    assertThat(codeOf(manifest)).as("`${VAR}` in the StatefulSet reaches the JVM verbatim").doesNotContain("${");

    // $(VAR) is resolved only against the variables this container declares: anything else is left
    // as a literal too. HOSTNAME comes from the container runtime, not from the pod spec.
    final Set<String> declared = blockEntryNames(manifest, "env:");
    for (final String referenced : matches(EXPANSION, codeOf(manifest)))
      assertThat(declared).as("$(%s) is not declared in the container env", referenced).contains(referenced);
  }

  @Test
  void statefulSetOnlyPassesSettingsThatStillExist() {
    for (final String key : matches(SETTING, codeOf(read(STATEFULSET))))
      assertThat(GlobalConfiguration.findByKey(key)).as("setting '%s' no longer exists: the argument is a no-op", key)
          .isNotNull();
  }

  @Test
  void statefulSetWiresTheRaftPortTheServerBinds() {
    final String manifest = read(STATEFULSET);
    final String raftPort = String.valueOf(GlobalConfiguration.HA_RAFT_PORT.getDefValue());

    // Peer list entries are host:raftPort:httpPort; the HTTP port is what replicas forward commands to.
    final Matcher serverList = Pattern.compile("-Darcadedb\\.ha\\.serverList=([^\"\\s]+)").matcher(manifest);
    assertThat(serverList.find()).as("the StatefulSet no longer configures a peer list").isTrue();
    final String[] entry = serverList.group(1).split(":");
    assertThat(entry).as("peer list entry must carry both the Raft and the HTTP port").hasSize(3);
    assertThat(entry[1]).as("peer list points at a port the server does not bind").isEqualTo(raftPort);
    // The HTTP listener default is a range (first free port wins), and the peer list has to name the
    // one the pod actually gets: with a single server per pod that is always the first of the range.
    final String httpPortRange = String.valueOf(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT.getDefValue());
    assertThat(entry[2]).isEqualTo(httpPortRange.split("-")[0]);
    assertThat(manifest).contains("- containerPort: " + entry[2] + "\n              name: http");

    // The Service and the container port have to agree with it, or nothing reaches the listener.
    assertThat(manifest).contains("- name: raft\n      port: " + raftPort);
    assertThat(manifest).contains("- containerPort: " + raftPort + "\n              name: raft");
  }

  @Test
  void statefulSetKubernetesDnsSuffixMatchesItsOwnServiceAndNamespace() {
    final String manifest = read(STATEFULSET);

    final Set<String> namespaces = new LinkedHashSet<>(
        matches(Pattern.compile("^\\s*namespace: (\\S+)\\s*$", Pattern.MULTILINE), manifest));
    assertThat(namespaces).as("every resource must live in the same namespace").hasSize(1);

    final Matcher serviceName = Pattern.compile("^\\s*serviceName: (\\S+)\\s*$", Pattern.MULTILINE).matcher(manifest);
    assertThat(serviceName.find()).isTrue();

    // A pod of a StatefulSet is reachable as <pod>.<headless service>.<namespace>.svc.cluster.local:
    // the suffix appended to short peer names has to spell exactly that, or DNS never resolves.
    assertThat(manifest).contains(
        "-Darcadedb.ha.k8sSuffix=." + serviceName.group(1) + "." + namespaces.iterator().next() + ".svc.cluster.local");
  }

  @Test
  void statefulSetReadsTheRootPasswordFromTheMountedSecret() {
    final String manifest = read(STATEFULSET);

    final Matcher path = Pattern.compile("-Darcadedb\\.server\\.rootPasswordPath=(\\S+?)\"").matcher(manifest);
    assertThat(path.find()).as("the root password must not be passed as a command-line argument").isTrue();
    assertThat(manifest).as("the root password must not be passed as a command-line argument")
        .doesNotContain("-Darcadedb.server.rootPassword=");

    final String directory = path.group(1).substring(0, path.group(1).lastIndexOf('/'));
    assertThat(matches(MOUNT_PATH, manifest)).as("no volume is mounted at the configured password directory")
        .contains(directory);
  }

  @Test
  void statefulSetProbesTheEndpointTheServerRegisters() {
    final String manifest = read(STATEFULSET);
    // Registered unauthenticated in HttpServer as /api/v1 + /ready.
    assertThat(manifest).contains("path: /api/v1/ready");
    // Readiness gated on Raft membership means a starting pod is not Ready, and a headless service
    // hides the DNS record of a not-Ready pod unless told otherwise: without this the node can never
    // be dialled by its peers, so it can never become Ready.
    assertThat(manifest).contains("publishNotReadyAddresses: true");
    assertThat(manifest).contains("-Darcadedb.server.readinessRequiresHA=true");
  }

  @Test
  void dockerImageSizesTheHeapAgainstTheContainerLimit() {
    final String memory = envValue(read(DOCKERFILE), "ARCADEDB_OPTS_MEMORY");

    // A pinned heap larger than the cgroup limit kills the container before the listener binds.
    assertThat(memory).doesNotContain("-Xmx").doesNotContain("-Xms").contains("MaxRAMPercentage");
  }

  @Test
  void dockerImageKeepsItsGarbageCollectorOutOfJavaOpts() {
    final String dockerfile = read(DOCKERFILE);

    // JAVA_OPTS is what a user overrides to add JVM flags, and an environment variable is replaced
    // rather than appended to, so anything the image parks there is lost exactly when it is customised.
    assertThat(dockerfile).as("the image must not own JAVA_OPTS").doesNotContain("ENV JAVA_OPTS=");
    assertThat(envValue(dockerfile, "ARCADEDB_OPTS_GC")).contains("UseZGC");

    // ...and the variable is only useful if the launchers actually expand it.
    assertThat(read(SERVER_SH)).contains("$ARCADEDB_OPTS_GC");
    assertThat(read(SERVER_BAT)).contains("%ARCADEDB_OPTS_GC%");
  }

  @Test
  void dockerImageExposesTheRaftPort() {
    final String dockerfile = read(DOCKERFILE);
    assertThat(dockerfile).contains("EXPOSE " + GlobalConfiguration.HA_RAFT_PORT.getDefValue());
    assertThat(dockerfile).as("2424 is the removed binary protocol port; nothing listens on it").doesNotContain("2424");
  }

  @Test
  void quickstartsPassDatabaseSettingsThroughArcadedbSettings() {
    for (final String file : List.of(COMPOSE, README)) {
      final String content = read(file);
      assertThat(content).as("%s must document ARCADEDB_SETTINGS", file).contains("ARCADEDB_SETTINGS");
      for (final String line : content.split("\n"))
        assertThat(line.contains("JAVA_OPTS") && line.contains("-Darcadedb.")).as(
            "%s passes database settings through JAVA_OPTS, discarding the image's own JVM flags: %s", file, line).isFalse();
    }
  }

  @Test
  void composeMountsEveryVolumeItDeclares() {
    final String compose = read(COMPOSE);

    // A declared-but-unmounted volume leaves the image's VOLUME resolving to a throwaway anonymous
    // one, so the database silently disappears on the next `down`/`up` while the named volume stays empty.
    for (final String declared : blockEntryKeys(compose, "volumes:"))
      assertThat(compose).as("named volume '%s' is declared but never mounted", declared)
          .contains("- " + declared + ":/");

    assertThat(compose).as("`version` is obsolete under Compose v2 and warns on every `up`")
        .doesNotContain("\nversion:");
    assertThat(compose).as("without a healthcheck a crash-looping container still reports as running")
        .contains("healthcheck:");
  }

  /**
   * Value of a Dockerfile {@code ENV NAME="..."}, joining the backslash continuations.
   */
  private static String envValue(final String dockerfile, final String name) {
    final Matcher m = Pattern.compile("^ENV " + name + "=\"((?:[^\"\\\\]|\\\\.|\\\\\\n)*)\"", Pattern.MULTILINE)
        .matcher(dockerfile);
    assertThat(m.find()).as("the Dockerfile does not define %s", name).isTrue();
    return m.group(1);
  }

  /**
   * Names of the {@code - name: X} entries of the block introduced by the given key, scoped by
   * indentation so a same-named entry in a sibling block cannot satisfy the lookup.
   */
  private static Set<String> blockEntryNames(final String yaml, final String blockKey) {
    return collectBlock(yaml, blockKey, ENTRY_NAME);
  }

  /**
   * Keys of the mapping introduced by the given top-level block, e.g. the named volumes of a Compose file.
   */
  private static Set<String> blockEntryKeys(final String yaml, final String blockKey) {
    return collectBlock(yaml, blockKey, Pattern.compile("^\\s*(\\S+):\\s*$"));
  }

  private static Set<String> collectBlock(final String yaml, final String blockKey, final Pattern entry) {
    final Set<String> found = new LinkedHashSet<>();
    final String[] lines = yaml.split("\n");
    for (int i = 0; i < lines.length; i++) {
      if (!lines[i].strip().equals(blockKey))
        continue;

      final int blockIndent = indentOf(lines[i]);
      for (int j = i + 1; j < lines.length; j++) {
        if (lines[j].isBlank())
          continue;
        if (indentOf(lines[j]) <= blockIndent)
          break;
        final Matcher m = entry.matcher(lines[j]);
        if (m.matches())
          found.add(m.group(1));
      }
    }
    return found;
  }

  private static int indentOf(final String line) {
    int i = 0;
    while (i < line.length() && line.charAt(i) == ' ')
      ++i;
    return i;
  }

  /**
   * The file with its comment lines removed, so a port or a variable quoted in prose cannot fail a check.
   */
  private static String codeOf(final String yaml) {
    final StringBuilder buffer = new StringBuilder(yaml.length());
    for (final String line : yaml.split("\n"))
      if (!line.strip().startsWith("#"))
        buffer.append(line).append('\n');
    return buffer.toString();
  }

  private static List<String> matches(final Pattern pattern, final String content) {
    final List<String> found = new ArrayList<>();
    final Matcher m = pattern.matcher(content);
    while (m.find())
      found.add(m.group(1));
    return found;
  }

  private static String read(final String relativePath) {
    try {
      return Files.readString(repositoryRoot().resolve(relativePath));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Surefire runs with the module directory as working directory, so the shipped manifests live a
   * couple of levels up. Walk until the directory that holds them all.
   */
  private static Path repositoryRoot() {
    Path candidate = new File("").getAbsoluteFile().toPath();
    while (candidate != null) {
      if (Files.isRegularFile(candidate.resolve(STATEFULSET)) && Files.isRegularFile(candidate.resolve(COMPOSE)))
        return candidate;
      candidate = candidate.getParent();
    }
    throw new IllegalStateException("Cannot locate the repository root from " + new File("").getAbsolutePath());
  }
}
