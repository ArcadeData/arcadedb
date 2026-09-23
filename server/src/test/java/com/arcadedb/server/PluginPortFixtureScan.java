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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Source-level scan behind the guards that keep the wire-protocol test fixtures off hand-picked ports (issues #7496,
 * #8203, #8204, #8209). A fixed listener port is answered by whatever already holds it - a server from an earlier class
 * that has not released it, a developer's own ArcadeDB or Neo4j, another worktree's build - and the failure lands on a
 * different test every run, as "Address already in use" or, worse, as a call answered by a stranger. No behavioural
 * test can see that from inside one class, so the line is held by reading the sources.
 * <p>
 * A test class <i>starts the plugin</i> when its source carries the plugin's server-plugin entry, e.g.
 * {@code "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin"}. Every such class must be one of:
 * <ul>
 *   <li>a single-server fixture: a subclass, direct or not, of one of the {@code ephemeralBases}, which start the
 *   plugin on port {@code 0} and expose the port it actually bound;</li>
 *   <li>a Raft cluster fixture: a subclass of {@code BaseRaftHATest} that calls {@code allocateFixturePorts(...)}.</li>
 * </ul>
 * and none of them may pin the port with a literal - neither a {@code int ...GRPC_PORT... = 50051} constant, nor the
 * setting assigned a non-zero number - nor dial {@code "localhost", <production default>}.
 */
public final class PluginPortFixtureScan {

  /** The Raft cluster base class, which hands out every fixture port from one ledger (issue #8203). */
  public static final String RAFT_HA_BASE = "BaseRaftHATest";

  private static final Pattern CLASS_DECLARATION = Pattern.compile(
      "(?m)^(?:public\\s+|protected\\s+|abstract\\s+|final\\s+)*class\\s+(\\w+)(?:<[^>{]*>)?\\s+extends\\s+([\\w.]+)");

  /**
   * @param fixtures  every file that starts the plugin, i.e. what the scan actually checked
   * @param offenders one line per violation: the file and the rule it breaks
   *                  <p>
   *                  Files are named by their path under the scanned root, with {@code /} separators.
   */
  public record Result(List<String> fixtures, List<String> offenders) {
  }

  private PluginPortFixtureScan() {
  }

  /**
   * @param testSources     root of the test sources to walk
   * @param pluginClassName fully qualified name of the plugin, as it appears in the server-plugin entry
   * @param portSetting     the {@code GlobalConfiguration} constant name of the plugin's port, e.g. {@code GRPC_PORT}
   * @param defaultPort     the production default of that setting, which no test may dial
   * @param ephemeralBases  simple names of the base classes that start the plugin on an operating-system-assigned port
   */
  public static Result scan(final Path testSources, final String pluginClassName, final String portSetting, final int defaultPort,
      final Set<String> ephemeralBases) throws IOException {
    final Map<String, String> sources = new TreeMap<>();
    try (final Stream<Path> files = Files.walk(testSources)) {
      for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList())
        // Keyed by the path under the root, not the file name: two classes with the same simple name in different
        // packages would otherwise overwrite each other, and one fixture would drop out of the scan silently.
        sources.put(testSources.relativize(file).toString().replace('\\', '/'), Files.readString(file, StandardCharsets.UTF_8));
    }

    // Top-level class -> superclass, so a fixture that inherits from an intermediate abstract class still resolves. The
    // scan is textual and cannot follow imports, so a simple name declared in more than one package cannot be resolved:
    // such a name is recorded as ambiguous and any fixture whose chain reaches it is reported, never guessed.
    final Map<String, String> superclasses = new HashMap<>();
    final Set<String> ambiguous = new HashSet<>();
    for (final String source : sources.values()) {
      final Matcher declaration = CLASS_DECLARATION.matcher(source);
      if (declaration.find() && superclasses.put(declaration.group(1), simpleName(declaration.group(2))) != null)
        ambiguous.add(declaration.group(1));
    }

    final Pattern startsPlugin = Pattern.compile("\"[\\w-]+:" + Pattern.quote(pluginClassName) + "\"");
    // `int BASE_GRPC_PORT = 51141`, `Integer GRPC_PORT = 50051`: a port picked by hand and kept in a constant.
    final Pattern literalConstant = Pattern.compile("\\b(?:int|Integer)\\s+\\w*" + Pattern.quote(portSetting) + "\\w*\\s*=\\s*\\d");
    // `GRPC_PORT.setValue(50051)`, `setValue(GlobalConfiguration.GRPC_PORT, 50081)`, `GRPC_PORT.getKey(), "50051"`.
    // Zero is allowed: it is how the ephemeral bases ask the operating system for a free port.
    final Pattern literalSetting = Pattern.compile(
        "\\b" + Pattern.quote(portSetting) + "(?:\\.getKey\\(\\))?\\s*(?:\\.setValue\\(|,)\\s*\"?[1-9]");
    final Pattern dialsDefault = Pattern.compile("\"localhost\"\\s*,\\s*" + defaultPort + "\\b");

    final List<String> fixtures = new ArrayList<>();
    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, String> entry : sources.entrySet()) {
      final String name = entry.getKey();
      final String source = entry.getValue();
      if (!startsPlugin.matcher(source).find())
        continue;
      fixtures.add(name);

      final Matcher declaration = CLASS_DECLARATION.matcher(source);
      // Start from this file's own declared parent: the name-keyed map may hold a same-named class of another package.
      final String parent = declaration.find() ? simpleName(declaration.group(2)) : null;

      final String unresolvable = firstAmbiguousAncestor(parent, superclasses, ambiguous);
      if (unresolvable != null) {
        offenders.add(name + ": extends a chain through " + unresolvable
            + ", which is declared in more than one package, so the scan cannot tell which base it reaches");
        continue;
      }

      if (inherits(parent, RAFT_HA_BASE, superclasses)) {
        if (!source.contains("allocateFixturePorts("))
          offenders.add(name + ": a Raft cluster fixture that does not take its ports from allocateFixturePorts");
      } else if (ephemeralBases.stream().noneMatch(base -> inherits(parent, base, superclasses)))
        offenders.add(name + ": starts the plugin outside " + ephemeralBases + " and " + RAFT_HA_BASE
            + ", so nothing assigns it a free port");

      if (literalConstant.matcher(source).find())
        offenders.add(name + ": keeps a hand-picked " + portSetting + " in a constant");
      if (literalSetting.matcher(source).find())
        offenders.add(name + ": sets " + portSetting + " to a hand-picked number");
      if (dialsDefault.matcher(source).find())
        offenders.add(name + ": dials the production default port " + defaultPort);
    }
    return new Result(fixtures, offenders);
  }

  /** The first name on {@code parent}'s ancestor chain that the tree declares more than once, or null. */
  private static String firstAmbiguousAncestor(final String parent, final Map<String, String> superclasses,
      final Set<String> ambiguous) {
    String current = parent;
    for (int depth = 0; current != null && depth <= superclasses.size(); depth++) {
      if (ambiguous.contains(current))
        return current;
      current = superclasses.get(current);
    }
    return null;
  }

  /** {@code com.arcadedb.server.BaseGraphServerTest} -> {@code BaseGraphServerTest}; a simple name is returned as is. */
  private static String simpleName(final String typeName) {
    return typeName.substring(typeName.lastIndexOf('.') + 1);
  }

  /** Whether {@code parent}, or any ancestor of it declared in the scanned tree, is {@code base}. */
  private static boolean inherits(final String parent, final String base, final Map<String, String> superclasses) {
    String current = parent;
    // Bounded by the number of known classes, so a malformed cycle cannot spin.
    for (int depth = 0; current != null && depth <= superclasses.size(); depth++) {
      if (base.equals(current))
        return true;
      current = superclasses.get(current);
    }
    return false;
  }
}
