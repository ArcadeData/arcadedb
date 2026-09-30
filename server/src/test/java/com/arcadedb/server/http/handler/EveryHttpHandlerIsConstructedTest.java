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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard: every concrete HTTP handler in this package is constructed by some production code (issue #7829).
 * <p>
 * {@code DeleteDropUserHandler} outlived the route that used to construct it. It still compiled and still called
 * {@code ServerSecurity.dropUserClusterWide}, so an audit of the security entry points counted it as a live door - twice -
 * and reached a wrong conclusion about the blast radius of a bug (issue #7559). A handler that no route builds is not
 * an endpoint; it is a false lead for the next reader. This test fails when one appears, so the route and its handler
 * are removed together.
 * <p>
 * The scan reads the main sources of every module, since plugins (gRPC, MCP, Grafana, ...) register handlers of this
 * package from outside the {@code server} module.
 */
class EveryHttpHandlerIsConstructedTest {

  /** The reactor root, seen from the {@code server} module that surefire runs this class in. */
  private static final Path REACTOR_ROOT = Path.of("..");

  private static final Path HANDLER_PACKAGE = REACTOR_ROOT.resolve(
      Path.of("server", "src", "main", "java", "com", "arcadedb", "server", "http", "handler"));

  /** A nested worktree is somebody else's checkout. */
  private static final List<String> SKIPPED_TOP_LEVEL = List.of(".worktrees", "node_modules");

  /** The package holds far more handlers than this; below it the scan has silently lost its root. */
  private static final int EXPECTED_MINIMUM_HANDLERS = 30;

  /** Comments and string/char literals, matched left to right so a {@code //} inside a string is not a comment. */
  private static final Pattern NOT_CODE = Pattern.compile(
      "//[^\\n]*|/\\*.*?\\*/|\"(?:\\\\.|[^\"\\\\\\n])*\"|'(?:\\\\.|[^'\\\\\\n])*'", Pattern.DOTALL);

  private static final Pattern CONSTRUCTION = Pattern.compile(
      "\\bnew\\s+(\\w+Handler)\\s*\\(|\\b(\\w+Handler)::new\\b|\\bextends\\s+(\\w+Handler)\\b");

  @Test
  void everyConcreteHandlerIsConstructedByProductionCode() throws IOException {
    assertThat(HANDLER_PACKAGE).as("the scan must start at the reactor root, or it checks nothing").isDirectory();

    final Set<String> constructed = constructedHandlers(readMainSources());
    final List<String> handlers = concreteHandlers();
    assertThat(handlers).as("the scan must find the handlers of this package").hasSizeGreaterThan(EXPECTED_MINIMUM_HANDLERS);
    // Sanity: a handler known to be routed must be seen as constructed, or the check below proves nothing.
    assertThat(constructed).as("the scan must see HttpServer's routes").contains("DeleteUserHandler");

    final List<String> unconstructed = new ArrayList<>(handlers);
    unconstructed.removeAll(constructed);

    assertThat(unconstructed)
        .as("a handler no route constructs is dead code that reads as a live endpoint: register it in HttpServer or delete it")
        .isEmpty();
  }

  private static List<String> concreteHandlers() throws IOException {
    final List<String> handlers = new ArrayList<>();
    try (final Stream<Path> files = Files.list(HANDLER_PACKAGE)) {
      for (final Path file : files.filter(p -> p.getFileName().toString().endsWith("Handler.java")).toList()) {
        final String name = file.getFileName().toString().replace(".java", "");
        final String source = Files.readString(file, StandardCharsets.UTF_8);
        // Abstract bases are skipped: only a class that can be instantiated has to be.
        if (Pattern.compile("^\\s*(?:public\\s+)?(?:final\\s+)?class\\s+" + name + "\\b", Pattern.MULTILINE).matcher(source).find())
          handlers.add(name);
      }
    }
    return handlers;
  }

  /** Every handler named by {@code new X(}, {@code X::new} or {@code extends X} in a main source other than X's own file. */
  private static Set<String> constructedHandlers(final Map<String, String> sources) {
    final Set<String> constructed = new HashSet<>();
    for (final Map.Entry<String, String> entry : sources.entrySet()) {
      // A "new FooHandler(" in a comment or a string literal constructs nothing.
      final Matcher use = CONSTRUCTION.matcher(NOT_CODE.matcher(entry.getValue()).replaceAll(" "));
      while (use.find()) {
        final String handler = use.group(1) != null ? use.group(1) : use.group(2) != null ? use.group(2) : use.group(3);
        if (!entry.getKey().endsWith("/" + handler + ".java"))
          constructed.add(handler);
      }
    }
    return constructed;
  }

  private static Map<String, String> readMainSources() throws IOException {
    final Map<String, String> sources = new TreeMap<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).toList()) {
        final String moduleName = module.getFileName().toString();
        if (SKIPPED_TOP_LEVEL.stream().anyMatch(moduleName::startsWith))
          continue;
        final Path mainSources = module.resolve("src").resolve("main").resolve("java");
        if (!Files.isDirectory(mainSources))
          continue;
        try (final Stream<Path> files = Files.walk(mainSources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList())
            sources.put(REACTOR_ROOT.relativize(file).toString().replace('\\', '/'), Files.readString(file, StandardCharsets.UTF_8));
        }
      }
    }
    return sources;
  }
}
