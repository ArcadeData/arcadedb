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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the shipped deployment examples honest against the code they launch. They are the first
 * thing an evaluator runs, they are never exercised by any other test, and every defect they grew
 * (issues #6840, #6841, #6842) was a silent divergence from a setting, a port or a variable that had
 * moved on: a Raft port the image stopped exposing, a heap pinned above the container limit, a garbage
 * collector discarded by the quickstart that overwrites the variable holding it, and a named volume
 * declared but never mounted. The Kubernetes deployment lives in the Helm chart repository
 * (ArcadeData/arcadedb-helm), whose own tests guard it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PackagingManifestsTest {

  private static final String DOCKERFILE  = "package/src/main/docker/Dockerfile";
  private static final String BUILDER     = "package/arcadedb-builder.sh";
  private static final String NATIVE_SCRATCH    = "native/src/main/docker/Dockerfile.native.scratch";
  private static final String NATIVE_DISTROLESS = "native/src/main/docker/Dockerfile.native.distroless";
  private static final String SERVER_SH   = "package/src/main/scripts/server.sh";
  private static final String SERVER_BAT  = "package/src/main/scripts/server.bat";
  private static final String COMPOSE     = "docker-compose.yml";
  private static final String README      = "README.md";
  private static final String DEPENDABOT  = ".github/dependabot.yml";
  private static final String CONFIG_DIR  = "package/src/main/config";
  private static final String FULL_ASSEMBLY      = "package/src/main/assembly/full.xml";
  private static final String NATIVE_WORKFLOW    = ".github/workflows/native-image.yml";
  private static final String NATIVE_DOCKER_SH   = "native/scripts/build-native-docker.sh";
  private static final String BUILDER_README     = "package/README-BUILDER.md";
  private static final String NATIVE_DOC         = "docs/native-image.md";

  /** The UID and GID every ArcadeDB image runs as, so their volumes are interchangeable (issue #10319). */
  private static final int    IMAGE_UID   = 1000;

  private static final Pattern BASE_IMAGE  = Pattern.compile("^FROM eclipse-temurin:(\\d+)-", Pattern.MULTILINE);
  private static final Pattern ARG_DEFAULT = Pattern.compile("^ARG ([A-Za-z_][A-Za-z0-9_]*)=(\\S+)\\s*$", Pattern.MULTILINE);

  private static final Pattern VOLUME      = Pattern.compile("^VOLUME\\s*\\[(.*)]\\s*$", Pattern.MULTILINE);
  private static final Pattern QUOTED      = Pattern.compile("\"([^\"]+)\"");
  /**
   * A Compose short-syntax mount of a named volume, {@code - <name>:<container path>[:<mode>]}. A bind mount
   * ({@code ./dir:/path}) or a long-syntax entry ({@code type: volume}, {@code source:}, {@code target:}) does not
   * match: switching the quickstart to long syntax needs this pattern extended, or every mount reads as missing.
   */
  private static final Pattern COMPOSE_MOUNT = Pattern.compile("(?m)^\\s*- ([A-Za-z0-9][A-Za-z0-9_.-]*):(/[^:\\s]+)(?::\\w+)?\\s*$");

  @Test
  void dockerImagesSizeTheHeapAgainstTheContainerLimit() {
    jvmDockerfiles().forEach((name, dockerfile) -> {
      // A pinned heap larger than the cgroup limit kills the container before the listener binds.
      assertThat(envValue(dockerfile, "ARCADEDB_OPTS_MEMORY")).as("%s pins the heap", name).doesNotContain("-Xmx")
          .doesNotContain("-Xms").contains("MaxRAMPercentage");
    });
  }

  @Test
  void dockerImagesKeepTheirGarbageCollectorOutOfJavaOpts() {
    jvmDockerfiles().forEach((name, dockerfile) -> {
      // JAVA_OPTS is what a user overrides to add JVM flags, and an environment variable is replaced
      // rather than appended to, so anything an image parks there is lost when it is customised.
      assertThat(dockerfile).as("%s must not own JAVA_OPTS", name).doesNotContain("ENV JAVA_OPTS=");
      assertThat(envValue(dockerfile, "ARCADEDB_OPTS_GC")).as("%s", name).contains("UseZGC");
    });

    // ...and the variable is only useful if the launchers actually expand it.
    assertThat(read(SERVER_SH)).contains("$ARCADEDB_OPTS_GC");
    assertThat(read(SERVER_BAT)).contains("%ARCADEDB_OPTS_GC%");
  }

  @Test
  void dockerImagesRunOnAJvmThatAcceptsCompactObjectHeaders() {
    // bin/server.sh enables compact object headers (JEP 519) only when the JVM accepts the flag: Java 21 rejects it
    // and Java 24 gates it behind UnlockExperimentalVMOptions, so on an older base the probe fails and every
    // container silently runs without them (issue #9602).
    jvmDockerfiles().forEach((name, dockerfile) -> assertThat(baseJavaMajor(dockerfile)).as(
        "%s runs on a JVM where server.sh never enables compact object headers", name).isGreaterThanOrEqualTo(25));
    assertThat(read(SERVER_SH)).contains("$ARCADEDB_OPTS_HEADERS");
  }

  @Test
  void dockerImagesPassNoGcFlagTheirJvmRemoved() {
    // Generational ZGC is the only ZGC mode from Java 24: -XX:+ZGenerational is obsolete there and the JVM prints
    // "Ignoring option ZGenerational; support was removed in 24.0" on every start.
    jvmDockerfiles().forEach((name, dockerfile) -> {
      if (baseJavaMajor(dockerfile) >= 24)
        assertThat(envValue(dockerfile, "ARCADEDB_OPTS_GC")).as("%s", name).doesNotContain("ZGenerational");
    });
  }

  @Test
  void dependabotKeepsTrackingTheDockerBaseMajor() {
    // The ignore rule holds the base image on its major (a JDK major is a deliberate move, not a weekly bump). Left
    // below the major the Dockerfile uses, it ignores every tag of that major too, digest refreshes included, so
    // the base never receives a security update again. A future major bump of the Dockerfile therefore has to move
    // this rule in the same change, which is the point.
    final Matcher m = Pattern.compile(
        "dependency-name:\\s*[\"']?eclipse-temurin[\"']?\\s*\\n\\s*versions:\\s*\\[\\s*[\"']>\\s*(\\d+)[\"']\\s*]")
        .matcher(read(DEPENDABOT));
    assertThat(m.find()).as("%s no longer pins the eclipse-temurin major", DEPENDABOT).isTrue();
    assertThat(Integer.parseInt(m.group(1))).isEqualTo(baseJavaMajor(read(DOCKERFILE)));
  }

  @Test
  void dockerImagesExposeTheRaftPort() {
    final String raftPort = String.valueOf(GlobalConfiguration.HA_RAFT_PORT.getDefValue());
    // The native images ship the same port surface, so they drift the same way.
    allDockerfiles().forEach((name, dockerfile) -> {
      final List<String> exposed = new ArrayList<>();
      for (final String ports : matches(Pattern.compile("^EXPOSE (.+)$", Pattern.MULTILINE), dockerfile))
        exposed.addAll(List.of(ports.strip().split("\\s+")));

      assertThat(exposed).as("%s does not expose the Raft port", name).contains(raftPort);
      assertThat(exposed).as("2424 is the removed binary protocol port; nothing listens on it").doesNotContain("2424");
    });
  }

  @Test
  void jvmImagesRunAsTheUidTheNativeImagesShare() {
    // The JVM image's arcadedb user is the owner every native image's volumes are aligned to (issue #10319). Alpine's
    // adduser hands out 1000 to the first user only by convention, so pin it: an implicit 1000 drifts silently the day
    // the base image ships a user of its own. The custom-distribution image generated by the builder script is held to
    // the same owner (issue #10330): it created a system user, which Alpine resolves to 100:101, so its volumes could
    // not be opened by the official images without a chown.
    jvmDockerfiles().forEach((name, dockerfile) -> {
      // The builder-generated Dockerfile names its user through ARG ARCADEDB_USER=arcadedb; once its default is expanded
      // it must still read "arcadedb", so a renamed default fails here too.
      final String code = expandArgDefaults(codeOf(dockerfile));
      assertThat(code).as("%s must create the arcadedb user with an explicit UID", name)
          .containsPattern("(?m)^RUN (?:.*&& )?adduser .*-u " + IMAGE_UID + "\\b.* arcadedb(?: &&.*)?$");
      // Without -G, BusyBox adduser creates a same-named group whose GID is the UID. With an explicit group, that
      // group has to be pinned to the same id, or a system group lands on 101 and the GID drifts on its own. Neither
      // shipped Dockerfile uses -G today, so this branch only guards a future edit.
      final Matcher group = Pattern.compile("(?m)^RUN .*adduser .*-G (\\S+)").matcher(code);
      if (group.find())
        assertThat(code).as("%s must create the %s group with an explicit GID", name, group.group(1))
            .containsPattern("addgroup .*-g " + IMAGE_UID + "\\b.* " + Pattern.quote(group.group(1)) + "\\b");
      assertThat(code).as("%s must run as the arcadedb user", name).containsPattern("(?m)^USER arcadedb$");
    });
  }

  @Test
  void nativeImagesRunAsTheJvmImageUid() {
    // Issue #10319: the amd64 scratch image ran as root with root-owned volumes, so `--user 1000:1000` died on
    // ./log/arcadedb.log.0.lck, and the arm64 distroless one ran as 65532. Neither could swap volumes with the JVM
    // image. Every image now runs as 1000:1000 and owns everything it writes under /home/arcadedb.
    final String owner = IMAGE_UID + ":" + IMAGE_UID;
    for (final String path : List.of(NATIVE_SCRATCH, NATIVE_DISTROLESS)) {
      final String code = codeOf(read(path));

      assertThat(code).as("%s must run as %s", path, owner).containsPattern("(?m)^USER " + owner + "$");
      assertThat(code).as("%s must not depend on the distroless nonroot (65532) user", path).doesNotContain("65532")
          .doesNotContain("nonroot");

      // The home directory itself and bin/ are covered through the dirs stage: it creates them under /skel, chowns the
      // whole tree, and the `COPY --from=dirs` checked below lands it on /home/arcadedb with the same owner.
      final List<String> chowns = matches(Pattern.compile("chown\\s+(?:-R\\s+)?(\\S+)"), code);
      assertThat(chowns).as("%s must pre-create its directories", path).isNotEmpty();
      assertThat(chowns).as("%s chowns its directories to another owner", path).containsOnly(owner);

      // Anonymous volumes are initialized with the ownership of the image directory, so anything the image COPYs
      // into the home directory must land owned by the runtime UID - config included, which the server writes to.
      // Only the single-line shell form of COPY is parsed, so refuse the JSON-array and continued forms rather than
      // let one slip past the check unread.
      for (final String copy : matches(Pattern.compile("(?m)^(COPY .*)$"), code))
        assertThat(copy).as("%s: COPY form this check cannot read", path).doesNotContain(" [\"").doesNotEndWith("\\");
      final List<String> copies = matches(Pattern.compile("(?m)^(COPY .* /home/arcadedb\\S*)$"), code);
      assertThat(copies).as("%s copies nothing into /home/arcadedb", path).isNotEmpty();
      for (final String copy : copies)
        assertThat(copy).as("%s: COPY into the home directory without the runtime owner", path)
            .contains("--chown=" + owner);
      assertThat(copies).as("%s does not copy the config directory", path)
          .anySatisfy(copy -> assertThat(copy).endsWith(" /home/arcadedb/config"));
    }

    // scratch ships no /tmp at all; the image materializes one, and it must stay world-writable (sticky) so a
    // `--user <uid>` override other than 1000 can still write temporary files. Distroless inherits the base's /tmp.
    assertThat(codeOf(read(NATIVE_SCRATCH))).as("%s must ship a sticky, world-writable /tmp", NATIVE_SCRATCH)
        .containsPattern("(?m)^COPY .*--chmod=1777 \\S+ /tmp$");
  }

  @Test
  void everyImageDeclaresTheSameVolumes() {
    // Issue #10387: the images were aligned on one owner so a volume written by one opens in another (#10319, #10330),
    // but the custom-distribution image declared only databases, backups and log. Its config (where the server writes
    // server-users.jsonl, server-groups.json and server-api-tokens.json) and replication (HA state) lived in the
    // container layer and were lost on `docker rm`, while the official images keep them. The JVM image is the reference.
    final Set<String> reference = declaredVolumes(DOCKERFILE, read(DOCKERFILE));
    assertThat(reference).as("%s declares no volumes", DOCKERFILE)
        .contains("/home/arcadedb/config", "/home/arcadedb/databases", "/home/arcadedb/replication",
            "/home/arcadedb/backups", "/home/arcadedb/log");

    allDockerfiles().forEach((name, dockerfile) -> assertThat(declaredVolumes(name, dockerfile))
        .as("%s declares a different set of volumes than %s", name, DOCKERFILE).containsExactlyInAnyOrderElementsOf(reference));
  }

  @Test
  void builderUpgradeNoteNamesEveryVolumeTheImageDeclares() {
    // Issue #10387: the #10330 upgrade note told operators to chown databases, backups and log only. A config or
    // replication directory mounted by hand stayed owned by 100:101 and the 1000:1000 image could not write to it.
    final String readme = read(BUILDER_README);
    final int section = readme.indexOf("### Docker image user");
    assertThat(section).as("%s no longer documents the Docker image user", BUILDER_README).isNotNegative();
    // The next Markdown heading ends the section; a `#` comment inside a fenced shell block must not.
    final Matcher heading = Pattern.compile("(?m)^#{1,3} ").matcher(readme);
    final int end = heading.find(section + 1) ? heading.start() : readme.length();
    final String note = readme.substring(section, end);

    for (final String volume : declaredVolumes(BUILDER + " (generated)", generatedDockerfile())) {
      final String directory = volume.substring(volume.lastIndexOf('/') + 1);
      assertThat(note).as("the %s upgrade note does not name the '%s' volume", BUILDER_README, directory)
          .contains("`" + directory + "`");
    }
  }

  @Test
  void builderGeneratedImageOwnsEveryDirectoryItDeclaresAsAVolume() {
    // A VOLUME whose directory the image does not contain is created by Docker owned by root, which the 1000:1000 user
    // cannot write to. The builder therefore creates each declared directory in the distribution before the
    // `COPY --chown` that hands it to the runtime user.
    final String builder = read(BUILDER);
    final int start = builder.indexOf("\ngenerate_dockerfile() {");
    assertThat(start).as("%s no longer defines generate_dockerfile", BUILDER).isNotNegative();
    final int heredoc = builder.indexOf("<<'EOF'", start);
    assertThat(heredoc).as("%s: generate_dockerfile no longer writes a heredoc", BUILDER).isNotNegative();
    // Only the part of the function that runs before the heredoc is written, so a mkdir elsewhere cannot satisfy it.
    final String code = codeOf(builder.substring(start, heredoc));
    for (final String volume : declaredVolumes(BUILDER + " (generated)", generatedDockerfile())) {
      final String directory = volume.substring(volume.lastIndexOf('/') + 1);
      assertThat(code).as("%s does not create the '%s' directory it declares as a volume", BUILDER, directory)
          .containsPattern("mkdir -p .*\\b" + Pattern.quote(directory) + "\\b");
    }
    assertThat(expandArgDefaults(codeOf(generatedDockerfile())))
        .as("%s must copy the distribution owned by the runtime user", BUILDER)
        .containsPattern("(?m)^COPY --chown=arcadedb:arcadedb \\. /home/arcadedb$");
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

  @Test
  void composeMountsANamedVolumeOnEveryVolumeTheImageDeclares() {
    // Issue #10402: the quickstart mounted a named volume on databases only, so the image's config VOLUME (where the
    // server writes server-users.jsonl, server-groups.json and server-api-tokens.json) resolved to an anonymous one, and
    // `docker compose down -v` or a different project name silently dropped every user, group and API token created at
    // runtime. Every directory the image declares must land on a named volume the file also declares.
    final String compose = codeOf(read(COMPOSE));
    final Set<String> declared = blockEntryKeys(compose, "volumes:");
    final Map<String, String> mounts = new LinkedHashMap<>();
    final Matcher mount = COMPOSE_MOUNT.matcher(compose);
    while (mount.find())
      mounts.put(mount.group(2), mount.group(1));

    for (final String volume : declaredVolumes(DOCKERFILE, read(DOCKERFILE))) {
      assertThat(mounts).as("%s mounts no named volume (short syntax `- <name>:<path>`) on the image's '%s' volume, so it"
          + " resolves to an anonymous one", COMPOSE, volume).containsKey(volume);
      assertThat(declared).as("%s mounts '%s' on %s, which is not a named volume it declares", COMPOSE, mounts.get(volume),
          volume).contains(mounts.get(volume));
    }
    assertThat(mounts.values()).as("%s mounts one named volume on two directories", COMPOSE).doesNotHaveDuplicates();
  }

  @Test
  void nativeRunHintsKeepTheConfigVolumeAcrossRm() {
    // Issue #10402: `docker run --rm` deletes the anonymous volumes of the container it removes, so a hint that mounts
    // only databases loses every user, group and API token the server wrote to config. A named volume survives `--rm`
    // and, unlike a bind mount of an empty directory, is seeded with the image's default config files. The name may not
    // contain a '/', so a bind mount such as `-v ./config:/home/arcadedb/config` is rejected on purpose.
    for (final String hint : List.of(NATIVE_DOCKER_SH, NATIVE_DOC))
      assertThat(read(hint)).as("%s tells the reader to run the image without a named volume on config", hint)
          .containsPattern("-v \"?[A-Za-z0-9_.-]+:/home/arcadedb/config\"?");
  }

  @Test
  void everyConfigTemplateIsShippedByTheJvmDistribution() {
    // A file under package/src/main/config that no assembly ships is never exercised by the JVM build,
    // so nothing notices when it rots - which is how a pretty-printed 2021 server-users.jsonl outlived
    // its format and then reached the native image, where it failed to parse on every boot (issue #10320).
    final Set<String> includes = configIncludes();
    for (final String file : configFiles())
      assertThat(includes).as("%s/%s is shipped by no JVM distribution (%s ships %s)", CONFIG_DIR, file, FULL_ASSEMBLY,
          includes).anySatisfy(glob -> assertThat(file).endsWith(glob.substring(1)));
  }

  @Test
  void nativeImageStagesTheSameConfigFilesAsTheJvmDistribution() {
    // Both native staging steps used to copy the whole directory, so a file the JVM assemblies filter out
    // still landed in /home/arcadedb/config of the native image (issue #10320).
    final Set<String> includes = configIncludes();
    for (final String stager : List.of(NATIVE_WORKFLOW, NATIVE_DOCKER_SH)) {
      final List<String> copies = configCopyCommands(read(stager));
      assertThat(copies).as("%s no longer stages %s", stager, CONFIG_DIR).isNotEmpty();

      final Set<String> staged = new LinkedHashSet<>();
      for (final String copy : copies) {
        assertThat(copy).as("%s copies %s wholesale instead of the assemblies' include set: %s", stager, CONFIG_DIR, copy)
            .doesNotContainPattern("config/[\"]?(\\*|\\.)[\"]?(\\s|$)");
        staged.addAll(matches(Pattern.compile("config/[\"]?\\*(\\.[A-Za-z0-9]+)"), copy).stream().map(ext -> "*" + ext).toList());
      }
      assertThat(staged).as("%s stages a different include set than %s", stager, FULL_ASSEMBLY)
          .containsExactlyInAnyOrderElementsOf(includes);
    }
  }

  @Test
  void everyShippedConfigFileIsSeededIntoAMovedConfigDirectoryOrExcludedForAReason() {
    // Issue #10536: a server whose configuration directory was moved (a Kubernetes volume claim starts empty) copies the
    // shipped defaults into it at startup. A file added under package/src/main/config has to be placed on one side.
    // The JVM loads logging through -Djava.util.logging.config.file, never from the configuration directory, so a copy
    // there would be a file the operator edits to no effect.
    final Set<String> notSeeded = Set.of("arcadedb-log.properties", "arcadedb-log-all.properties");
    for (final String file : configFiles())
      assertThat(ConfigDirectorySeeder.SEEDED_FILES.contains(file) || notSeeded.contains(file))
          .as("%s/%s is neither seeded into a moved configuration directory nor excluded", CONFIG_DIR, file).isTrue();
    assertThat(configFiles()).as("the seeder names a file the distribution does not ship")
        .containsAll(ConfigDirectorySeeder.SEEDED_FILES);
  }

  @Test
  void shippedJsonConfigTemplatesParseTheWayTheServerReadsThem() {
    // server-groups.json is read as one document, server-users.jsonl as one object per line: a template
    // that does not parse makes the server log a SEVERE and fall back to its defaults on every boot.
    for (final String file : configFiles()) {
      final String content = read(CONFIG_DIR + "/" + file);
      if (file.endsWith(".json"))
        assertThat(new JSONObject(content).isEmpty()).as("%s is an empty document", file).isFalse();
      else if (file.endsWith(".jsonl"))
        for (final String line : content.split("\n"))
          if (!line.isBlank())
            assertThat(new JSONObject(line).isEmpty()).as("%s holds an empty record", file).isFalse();
    }
  }

  /**
   * The {@code <include>} globs of the fileSet that ships {@code src/main/config} in the full assembly.
   */
  private static Set<String> configIncludes() {
    final Matcher fileSet = Pattern.compile("<directory>\\$\\{basedir}/src/main/config</directory>(.*?)</fileSet>",
        Pattern.DOTALL).matcher(read(FULL_ASSEMBLY));
    assertThat(fileSet.find()).as("%s no longer ships %s", FULL_ASSEMBLY, CONFIG_DIR).isTrue();
    final Set<String> includes = new LinkedHashSet<>(
        matches(Pattern.compile("<include>([^<]+)</include>"), fileSet.group(1)));
    assertThat(includes).as("the config fileSet of %s has no include filter", FULL_ASSEMBLY).isNotEmpty()
        .allSatisfy(glob -> assertThat(glob).matches("\\*\\.[A-Za-z0-9]+"));
    return includes;
  }

  /**
   * Names of the files committed under {@link #CONFIG_DIR}.
   */
  private static List<String> configFiles() {
    try (final Stream<Path> files = Files.list(repositoryRoot().resolve(CONFIG_DIR))) {
      return files.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).sorted().toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The {@code cp} commands of a shell script (or of a workflow's {@code run:} block) that read from
   * {@link #CONFIG_DIR}, with backslash continuations joined.
   */
  private static List<String> configCopyCommands(final String script) {
    final List<String> found = new ArrayList<>();
    for (final String line : script.replace("\\\n", " ").split("\n")) {
      final String command = line.strip();
      if (command.startsWith("cp ") && command.contains(CONFIG_DIR + "/"))
        found.add(command);
    }
    return found;
  }

  /**
   * Every Dockerfile that ships a JVM ArcadeDB image, keyed by where it lives. The custom-distribution
   * one is generated by a heredoc inside the builder script rather than committed as a file, which is
   * exactly why it drifted out of step with the one next to it.
   */
  private static Map<String, String> jvmDockerfiles() {
    final Map<String, String> found = new LinkedHashMap<>();
    found.put(DOCKERFILE, read(DOCKERFILE));
    found.put(BUILDER + " (generated)", generatedDockerfile());
    return found;
  }

  /**
   * The JVM images plus the two native ones. The native bases have no shell and no launcher script, so
   * they carry no heap or GC variables to check - but they publish the same ports, so the port checks
   * apply to them too.
   */
  private static Map<String, String> allDockerfiles() {
    final Map<String, String> found = jvmDockerfiles();
    found.put(NATIVE_SCRATCH, read(NATIVE_SCRATCH));
    found.put(NATIVE_DISTROLESS, read(NATIVE_DISTROLESS));
    return found;
  }

  private static String generatedDockerfile() {
    final String builder = read(BUILDER);
    final int heredoc = builder.indexOf("<<'EOF'\nFROM ");
    assertThat(heredoc).as("%s no longer generates a Dockerfile through a heredoc", BUILDER).isNotNegative();

    final int from = builder.indexOf('\n', heredoc) + 1;
    final int to = builder.indexOf("\nEOF\n", from);
    assertThat(to).as("unterminated Dockerfile heredoc in %s", BUILDER).isNotNegative();
    return builder.substring(from, to + 1);
  }

  /**
   * The paths a Dockerfile declares through {@code VOLUME [ ... ]}, across every such line, with the {@code ARG}
   * defaults expanded so the builder's {@code ${ARCADEDB_HOME}/config} compares equal to {@code /home/arcadedb/config}.
   */
  private static Set<String> declaredVolumes(final String name, final String dockerfile) {
    final String code = expandArgDefaults(codeOf(dockerfile));
    for (final String line : code.split("\n"))
      if (line.startsWith("VOLUME"))
        assertThat(line).as("%s: VOLUME form this check cannot read", name).matches("^VOLUME\\s*\\[.*]\\s*$");

    final Set<String> found = new LinkedHashSet<>();
    for (final String list : matches(VOLUME, code))
      found.addAll(matches(QUOTED, list));
    assertThat(found).as("%s declares no volumes", name).isNotEmpty();
    return found;
  }

  /**
   * Java major version of the {@code eclipse-temurin} image a Dockerfile starts from.
   */
  private static int baseJavaMajor(final String dockerfile) {
    final Matcher m = BASE_IMAGE.matcher(dockerfile);
    assertThat(m.find()).as("the Dockerfile no longer starts from an eclipse-temurin image").isTrue();
    return Integer.parseInt(m.group(1));
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
   * Substitutes every {@code ${NAME}} with the default of its {@code ARG NAME=value} declaration, so a Dockerfile that
   * names its user through a build argument is checked against the user it actually creates when built as shipped.
   */
  private static String expandArgDefaults(final String dockerfile) {
    String expanded = dockerfile;
    final Matcher m = ARG_DEFAULT.matcher(dockerfile);
    while (m.find())
      expanded = expanded.replace("${" + m.group(1) + "}", m.group(2));
    return expanded;
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
      if (Files.isRegularFile(candidate.resolve(DOCKERFILE)) && Files.isRegularFile(candidate.resolve(COMPOSE)))
        return candidate;
      candidate = candidate.getParent();
    }
    throw new IllegalStateException("Cannot locate the repository root from " + new File("").getAbsolutePath());
  }
}
