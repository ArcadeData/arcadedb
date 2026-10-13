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
package com.arcadedb.mcp.tools;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9772: a tool argument is read through {@link MCPToolUtils}, whose accessors refuse a missing or wrongly shaped
 * member with a {@link com.arcadedb.mcp.MCPToolArgumentException}. A bare {@code args.getString("query")} raises a
 * {@code JSONException} instead, which production mode conceals and logs as a SEVERE server fault. This guard is what
 * stops a tool added later from reintroducing the shape: the accessors of the tool's own arguments object (named {@code args}, {@code arguments} or
 * {@code params}, with a literal key or not) are reached only from {@link MCPToolUtils}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9772ToolArgumentReadsTest {
  private static final Pattern ARGUMENT_READ = Pattern.compile("\\b(args|arguments|params)\\.(get[A-Za-z]*|opt[A-Za-z]*)\\(");

  @Test
  void noToolReadsAnArgumentWithTheRawAccessor() throws IOException {
    final Path tools = Path.of("src", "main", "java", "com", "arcadedb", "mcp", "tools");
    assertThat(tools).as("the test runs from the mcp module directory").isDirectory();

    final List<String> offenders = new ArrayList<>();
    try (final Stream<Path> files = Files.list(tools)) {
      for (final Path file : files.filter(f -> f.getFileName().toString().endsWith(".java")).sorted().toList()) {
        if (file.getFileName().toString().equals("MCPToolUtils.java"))
          continue;
        final List<String> lines = Files.readAllLines(file);
        for (int i = 0; i < lines.size(); i++) {
          final String line = lines.get(i);
          if (line.stripLeading().startsWith("//") || line.stripLeading().startsWith("*"))
            continue;
          if (ARGUMENT_READ.matcher(line).find())
            offenders.add(file.getFileName() + ":" + (i + 1) + "  " + line.strip());
        }
      }
    }

    assertThat(offenders).as("read the argument through MCPToolUtils (requireString, optionalString, intArgument, ...)").isEmpty();
  }
}
