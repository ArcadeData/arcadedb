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
package com.arcadedb;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #7233. A {@code SCOPE.SERVER} setting is authoritative in the SERVER's {@link ContextConfiguration}: the
 * server configuration file ({@link ContextConfiguration#fromJSON(String)}), {@code SET SERVER SETTING} and the MCP
 * {@code set_server_setting} tool all write there. The {@link GlobalConfiguration} enum is populated by
 * {@link GlobalConfiguration#readConfiguration()} alone, which consults a system property and then an environment
 * variable and nothing else.
 * <p>
 * So {@code GlobalConfiguration.X.getValueAsY()} on a {@code SCOPE.SERVER} setting, in code that runs inside a
 * server, silently ignores every channel the setting's own scope advertises except a raw {@code -D}. There is no
 * error and no warning; the value simply stays at the compiled-in default. That is how #7226 disabled a snapshot
 * size limit an operator had configured, and how the Studio production gate was decided by the default rather than
 * by what the deployment asked for.
 * <p>
 * The house pattern is {@code server.getConfiguration().getValueAsY(GlobalConfiguration.X)}, keeping the enum only
 * as the fallback for a caller with no server in reach. This test is what keeps that from being something
 * everybody has to remember: it walks every production source outside {@code engine/} and fails on a
 * {@code SCOPE.SERVER} read that goes through the enum without such a fallback beside it.
 * <p>
 * <b>What it does NOT catch, so nobody over-trusts it.</b> The check is syntactic - it matches source text, it does
 * not resolve types or data flow - and that leaves two holes:
 * <ol>
 *   <li>It reads {@code GlobalConfiguration.SETTING.getValueAsY()} literally, so a read INDIRECTED through a
 *       variable ({@code setting.getValueAsInteger()} inside a helper that takes the setting as a parameter) is
 *       invisible to it. Two such helpers existed when this was written - {@code RedisNetworkExecutor.sanitizedLimit}
 *       and {@code PackStreamReader.sanitizedLimit} - and both were converted by hand, not by this test.</li>
 *   <li>It cannot tell WHICH {@link ContextConfiguration} the nearby read is against. A decoy
 *       {@code new ContextConfiguration()} beside the enum read would satisfy the shape while reading exactly the
 *       same process-wide value, which is why that specific spelling is rejected below - but a decoy held in a
 *       field or a local variable still would not be.</li>
 * </ol>
 * So this is a ratchet against the ACCIDENTAL reintroduction of the idiom, which is how #7226 and #7233 happened;
 * it is not a proof that every server-scoped read resolves to a server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7233ServerScopeSettingReadsTest {

  /**
   * How far from the enum read the {@link ContextConfiguration} read of the SAME setting may be for the pair to
   * count as the ternary house pattern. Five lines covers every formatting of it in the tree, including the ones
   * with a comment between the two halves, without reaching into a neighbouring statement.
   */
  private static final int TERNARY_WINDOW_LINES = 5;

  /**
   * Reads that are deliberately off the enum, keyed {@code SimpleFileName.java#SETTING}. Each is a caller with no
   * server - and therefore no {@link ContextConfiguration} - anywhere in reach; the enum is the only thing there
   * is to read. Adding an entry here is a claim that has to be true of the site, not a way to silence the test.
   */
  private static final Map<String, String> ALLOWED = new HashMap<>();

  static {
    ALLOWED.put("Console.java#SERVER_ROOT_PATH",
        "CLI entry point: main() reads the root path to default it before anything builds a server");
    ALLOWED.put("Restore.java#SERVER_ROOT_PATH",
        "CLI entry point: same defaulting as Console.main, with no server in the process at all");
    ALLOWED.put("MCPStdioServer.java#SERVER_ROOT_PASSWORD",
        "Checked in main() to refuse stdio mode early; the ArcadeDBServer it guards is constructed after it");
    ALLOWED.put("ImportSecurityValidator.java#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS",
        "Static validator shared by the CLI importer; the server path resolves the policy itself and passes it "
            + "through the openRemoteConnection(url, blockLocalNetworks) overload (issue #6474)");
    ALLOWED.put("ImportSecurityValidator.java#SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS",
        "Static validator with no caller-resolved override yet; see the note on the issue");
    ALLOWED.put("SourceDiscovery.java#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS",
        "Fallback for a null allowLocalUrls, i.e. an embedded/CLI caller that resolved no policy of its own");
    ALLOWED.put("FullRestoreFormat.java#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS",
        "Fallback for a null settings.allowLocalUrls, same shape as SourceDiscovery above");
  }

  private static final Pattern SCOPE_DECLARATION = Pattern.compile(
      "^\\s{2}([A-Z][A-Z0-9_]*)\\(\"[^\"]*\",\\s*SCOPE\\.([A-Z]+)", Pattern.MULTILINE);
  private static final Pattern ENUM_READ         = Pattern.compile(
      "GlobalConfiguration\\.([A-Z0-9_]+)\\.getValueAs\\w*\\s*\\(");

  @Test
  public void noServerScopedSettingIsReadOffTheEnumWithoutAFallback() throws IOException {
    final Path repoRoot = findRepoRoot();
    assumeTrue(repoRoot != null, "sources not available in this run");

    final Map<String, String> scopes = readScopes(repoRoot);
    assertThat(scopes).as("SCOPE declarations parsed out of GlobalConfiguration").isNotEmpty();

    final List<String> violations = new ArrayList<>();
    final Set<String> allowedHit = new HashSet<>();

    for (final Path file : productionSourcesOutsideEngine(repoRoot)) {
      final List<String> lines = Files.readAllLines(file);
      for (int i = 0; i < lines.size(); i++) {
        final Matcher m = ENUM_READ.matcher(lines.get(i));
        while (m.find()) {
          final String setting = m.group(1);
          if (!"SERVER".equals(scopes.get(setting)))
            continue;

          final String key = file.getFileName() + "#" + setting;
          if (ALLOWED.containsKey(key)) {
            allowedHit.add(key);
            continue;
          }
          if (hasContextConfigurationReadNearby(lines, i, setting))
            continue;

          violations.add(repoRoot.relativize(file) + ":" + (i + 1) + " reads SCOPE.SERVER setting " + setting
              + " off the GlobalConfiguration enum");
        }
      }
    }

    assertThat(violations).as("""
        A SCOPE.SERVER setting read off the GlobalConfiguration enum sees only a system property or an environment \
        variable: the server configuration file, SET SERVER SETTING and the MCP set_server_setting tool all write \
        into the server's ContextConfiguration instead, and the read above ignores all three without a word \
        (issue #7233). Use server.getConfiguration().getValueAsX(GlobalConfiguration.SETTING), keeping the enum as \
        the fallback for a caller with no server - or, when there genuinely is none, add the site to ALLOWED in \
        this test with the reason.""").isEmpty();

    assertThat(new TreeSet<>(ALLOWED.keySet())).as(
        "stale ALLOWED entries: these sites no longer exist, so remove them rather than leave the next reader "
            + "believing the exception is still needed").isEqualTo(new TreeSet<>(allowedHit));
  }

  /**
   * The ternary house pattern: the same setting read through a {@link ContextConfiguration} within a few lines,
   * which is what {@code server != null ? server.getConfiguration().getValueAsX(S) : S.getValueAsX()} looks like.
   */
  private static boolean hasContextConfigurationReadNearby(final List<String> lines, final int index,
      final String setting) {
    final Pattern contextRead = Pattern.compile("getValueAs\\w*\\s*\\(\\s*GlobalConfiguration\\." + setting + "\\b");
    final int from = Math.max(0, index - TERNARY_WINDOW_LINES);
    final int to = Math.min(lines.size(), index + TERNARY_WINDOW_LINES + 1);
    for (int i = from; i < to; i++) {
      final String line = lines.get(i);
      // A freshly-built overlay reads exactly the process-wide value the enum read beside it does, so it is not a
      // fallback - it is the same answer written twice. Rejecting the spelling keeps it from being used to satisfy
      // the shape; see the class javadoc for the decoys this cannot see.
      if (line.contains("new ContextConfiguration()"))
        continue;
      if (contextRead.matcher(line).find())
        return true;
    }
    return false;
  }

  private static Map<String, String> readScopes(final Path repoRoot) throws IOException {
    final String source = Files.readString(
        repoRoot.resolve("engine/src/main/java/com/arcadedb/GlobalConfiguration.java"));
    final Map<String, String> scopes = new HashMap<>();
    final Matcher m = SCOPE_DECLARATION.matcher(source);
    while (m.find())
      scopes.put(m.group(1), m.group(2));
    return scopes;
  }

  private static List<Path> productionSourcesOutsideEngine(final Path repoRoot) throws IOException {
    final List<Path> sources = new ArrayList<>();
    try (Stream<Path> modules = Files.list(repoRoot)) {
      modules.filter(Files::isDirectory).filter(module -> !module.getFileName().toString().equals("engine"))
          .map(module -> module.resolve("src/main/java")).filter(Files::isDirectory).forEach(root -> {
            try (Stream<Path> walk = Files.walk(root)) {
              walk.filter(p -> p.getFileName().toString().endsWith(".java")).forEach(sources::add);
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }
    return sources;
  }

  /**
   * The repository root, found by walking up from the module the test runs in. Returns {@code null} when the
   * sources are not on disk, which is what the assumption above turns into a skip rather than a failure.
   */
  private static Path findRepoRoot() {
    Path candidate = Path.of("").toAbsolutePath();
    for (int i = 0; i < 4 && candidate != null; i++) {
      if (Files.isRegularFile(candidate.resolve("engine/src/main/java/com/arcadedb/GlobalConfiguration.java")))
        return candidate;
      candidate = candidate.getParent();
    }
    return null;
  }
}
