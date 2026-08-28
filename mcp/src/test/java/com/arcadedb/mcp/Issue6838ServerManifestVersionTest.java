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
package com.arcadedb.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #6838: {@code server.json} is the manifest the MCP registry publishes this server from, and nothing in the
 * build touched it. It declared {@code "version": "26.3.1"} and pinned {@code arcadedata/arcadedb:26.3.1} while the
 * project was at 26.9.1-SNAPSHOT, so a client installing from the registry resolved a container six releases old
 * while reading the tool surface of HEAD.
 * <p>
 * The release workflow now rewrites both fields (see {@code .github/workflows/mvn-release.yml}); this test is what
 * notices if that step is removed, reordered or only half applied.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6838ServerManifestVersionTest {
  /**
   * How many CalVer minors (year*12 + month) the published manifest may trail the development POM by. The release
   * workflow keeps it at 0 or 1 - the manifest names the last release, the POM the next SNAPSHOT - so this bound is
   * deliberately slack: it exists to catch drift of the reported magnitude (26.3 against 26.9, six apart) without
   * turning a quarter with no release into a red main.
   */
  private static final int MAX_MINOR_LAG = 3;

  private static final Pattern VERSION = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)$");

  @Test
  void theManifestVersionAndTheImageTagAgree() throws IOException {
    final JSONObject manifest = readManifest();
    final String version = manifest.getString("version");

    final JSONArray packages = manifest.getJSONArray("packages");
    assertThat(packages.length()).isGreaterThan(0);

    for (int i = 0; i < packages.length(); i++) {
      final String identifier = packages.getJSONObject(i).getString("identifier");
      final int tagSeparator = identifier.lastIndexOf(':');
      assertThat(tagSeparator)
          .withFailMessage("server.json package '%s' pins no explicit tag, so the registry resolves a floating image",
              identifier)
          .isGreaterThan(0);

      assertThat(identifier.substring(tagSeparator + 1))
          .withFailMessage("server.json declares version '%s' but package '%s' pins another tag; bump both together",
              version, identifier)
          .isEqualTo(version);
    }
  }

  @Test
  void theManifestNamesAReleaseAndNotASnapshot() throws IOException {
    final String version = readManifest().getString("version");

    assertThat(VERSION.matcher(version).matches())
        .withFailMessage("server.json version '%s' is not a released <major>.<minor>.<patch>; the registry cannot "
            + "resolve a SNAPSHOT container image", version)
        .isTrue();
  }

  @Test
  void theManifestDoesNotTrailTheProjectVersion() throws IOException {
    final String manifestVersion = readManifest().getString("version");
    final String projectVersion = readProjectVersion();

    final int lag = calendarMinor(projectVersion) - calendarMinor(manifestVersion);

    assertThat(lag)
        .withFailMessage("server.json publishes the MCP server as '%s' while the project is at '%s'. The release "
                + "workflow rewrites server.json - check that step still runs.", manifestVersion, projectVersion)
        .isBetween(0, MAX_MINOR_LAG);
  }

  /** CalVer position of a version, counted in months, so 26.12 -> 27.1 is a distance of one rather than -11. */
  private static int calendarMinor(final String version) {
    final Matcher matcher = VERSION.matcher(stripSnapshot(version));
    if (!matcher.matches())
      return fail("Cannot parse version '%s' as <major>.<minor>.<patch>", version);

    return Integer.parseInt(matcher.group(1)) * 12 + Integer.parseInt(matcher.group(2));
  }

  private static String stripSnapshot(final String version) {
    final int dash = version.indexOf('-');
    return dash < 0 ? version : version.substring(0, dash);
  }

  private static JSONObject readManifest() throws IOException {
    return new JSONObject(Files.readString(repositoryRoot().resolve("server.json")));
  }

  /**
   * Reads the reactor root's own {@code <version>}. The root POM is {@code arcadedb-parent} itself and declares no
   * {@code <parent>}, so today that is simply the first {@code <version>} in the file; the {@code </parent>} skip is
   * there so the answer stays the root's own version rather than an inherited one if that ever changes. Parsing it
   * textually keeps the test free of an XML dependency and of any Maven-injected property, so it asserts against the
   * file a release actually rewrites.
   */
  private static String readProjectVersion() throws IOException {
    final String pom = Files.readString(repositoryRoot().resolve("pom.xml"));
    final int afterParent = pom.indexOf("</parent>");
    final Matcher matcher = Pattern.compile("<version>([^<]+)</version>")
        .matcher(afterParent < 0 ? pom : pom.substring(afterParent));

    if (!matcher.find())
      return fail("No <version> element found in the reactor root pom.xml");

    return matcher.group(1).trim();
  }

  /** Walks up from the module directory to the checkout holding server.json, so the test runs from any module. */
  private static Path repositoryRoot() {
    Path current = Paths.get("").toAbsolutePath();
    while (current != null) {
      if (Files.isRegularFile(current.resolve("server.json")) && Files.isRegularFile(current.resolve("pom.xml")))
        return current;
      current = current.getParent();
    }
    return fail("Cannot locate the repository root: no ancestor of %s holds both server.json and pom.xml",
        Paths.get("").toAbsolutePath());
  }
}
