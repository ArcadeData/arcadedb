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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7233. A {@code SCOPE.SERVER} setting is authoritative in the SERVER's {@code ContextConfiguration}: the server
 * configuration file, {@code SET SERVER SETTING} and the MCP {@code set_server_setting} tool all write there. The
 * {@code GlobalConfiguration} enum is populated by {@code readConfiguration()} alone, which consults a system property and then an
 * environment variable and nothing else. So {@code GlobalConfiguration.X.getValueAsY()} on a {@code SCOPE.SERVER} setting, in code
 * that runs inside a server, silently ignores every channel the setting's own scope advertises except a raw {@code -D}: that is how
 * #7226 disabled a snapshot size limit an operator had configured. The house pattern is
 * {@code server.getConfiguration().getValueAsY(GlobalConfiguration.X)}, keeping the enum only as the fallback for a caller with no
 * server in reach.
 * <p>
 * The census ranges over the authority (#11288): the scope of each setting is the one the enum declares ({@code getScope()}), and
 * the reads are every {@code getValue...} of a {@code GlobalConfiguration} constant, held in a local first or not, in the compiled
 * classes of every module but {@code engine} ({@link CompiledClasses}, {@link CallOperands}). A read is the fallback of the house
 * pattern when the same method also reads the same setting, by the constant or by its key, through a {@code ContextConfiguration}
 * that is not freshly built (a
 * {@code new ContextConfiguration()} reads exactly the process-wide value the enum read beside it does). The source-text census it
 * replaces parsed the scopes out of {@code GlobalConfiguration.java} with a regular expression, matched the spelling
 * {@code GlobalConfiguration.X.getValueAs(} line by line, and took a {@code ContextConfiguration} read within five lines as the
 * fallback.
 * <p>
 * What it does not catch, so nobody over-trusts it: a read indirected through a helper that takes the setting as a parameter
 * ({@code setting.getValueAsInteger()}) does not name the constant at the read, and a decoy {@code ContextConfiguration} held in a
 * field still counts as the fallback. A ratchet against the accidental reintroduction of the idiom, not a proof that every
 * server-scoped read resolves to a server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ServerScopeSettingReadsCensusTest {
  private static final String SETTINGS = "com/arcadedb/GlobalConfiguration";
  private static final String CONTEXT  = "com/arcadedb/ContextConfiguration";
  private static final String ENGINE   = "engine";

  /**
   * Reads deliberately off the enum, each a caller with no server - and therefore no {@code ContextConfiguration} - in reach. An
   * entry is a claim that has to be true of the site, not a way to silence the census.
   */
  private static final Map<String, String> NO_SERVER_IN_REACH = new TreeMap<>(Map.of(//
      "console/Console#main", "CLI entry point: main() reads the root path to default it before anything builds a server",//
      "integration/restore/Restore#main", "CLI entry point: same defaulting as Console.main, with no server in the process at all",//
      "integration/importer/ImportSecurityValidator#openRemoteConnection",
      "static validator shared by the CLI importer; the server path resolves the policy itself and passes it through the "
          + "openRemoteConnection(url, blockLocalNetworks) overload (issue #6474)",//
      "integration/importer/ImportSecurityValidator#validateRemoteURL", "the same validator, for the URL itself",//
      "integration/importer/ImportSecurityValidator#validateLocalURL",
      "static validator with no caller-resolved override yet; see the note on the issue",//
      "integration/importer/SourceDiscovery#getSourceFromURL",
      "fallback for a null allowLocalUrls, i.e. an embedded/CLI caller that resolved no policy of its own",//
      "integration/restore/format/FullRestoreFormat#openInputFile",
      "fallback for a null settings.allowLocalUrls, same shape as SourceDiscovery"));

  @Test
  void noServerScopedSettingIsReadOffTheEnumWithoutAFallback() throws Exception {
    final Map<String, byte[]> classes = new HashMap<>();
    for (final Map.Entry<String, Map<String, byte[]>> module : CompiledClasses.byModule().entrySet())
      if (!ENGINE.equals(module.getKey()))
        classes.putAll(module.getValue());

    final List<CallOperands.Site> reads = CallOperands.of(classes, (owner, name, desc) -> name.startsWith("getValue") && (
        SETTINGS.equals(owner) || CONTEXT.equals(owner)));
    final Map<String, Set<String>> offFromTheEnum = new TreeMap<>();
    int serverScoped = 0;
    for (final CallOperands.Site read : reads) {
      if (!SETTINGS.equals(read.call().owner))
        continue;
      final String setting = constantOf(read.operand(0));
      if (setting == null || GlobalConfiguration.valueOf(setting).getScope() != GlobalConfiguration.SCOPE.SERVER)
        continue;
      ++serverScoped;
      if (!hasAContextFallback(read, setting, reads))
        offFromTheEnum.computeIfAbsent(read.key(), k -> new TreeSet<>()).add(setting);
    }
    assertThat(serverScoped).as("the census must reach the server-scoped reads of the enum, the fallbacks of the house pattern "
        + "included").isGreaterThan(10);

    assertThat(CompiledClasses.unreviewed(offFromTheEnum, NO_SERVER_IN_REACH)).as(
        "a SCOPE.SERVER setting read off the GlobalConfiguration enum sees only a system property or an environment variable: the "
            + "server configuration file, SET SERVER SETTING and the MCP set_server_setting tool all write into the server's "
            + "ContextConfiguration instead (issue #7233). Use server.getConfiguration().getValueAsX(GlobalConfiguration.SETTING), "
            + "keeping the enum as the fallback for a caller with no server - or, when there genuinely is none, add the site to "
            + "NO_SERVER_IN_REACH with the reason").isEmpty();
    assertThat(CompiledClasses.stale(offFromTheEnum, NO_SERVER_IN_REACH)).as(
        "NO_SERVER_IN_REACH entries that no longer read off the enum: remove them").isEmpty();
  }

  /** Whether the method of {@code read} also reads {@code setting} through a {@code ContextConfiguration} it did not just build. */
  private static boolean hasAContextFallback(final CallOperands.Site read, final String setting, final List<CallOperands.Site> reads) {
    for (final CallOperands.Site other : reads)
      if (other.in() == read.in() && CONTEXT.equals(other.call().owner) && setting.equals(settingOf(other.in(), other.argument(0)))
          && other.operand(0).stream().noneMatch(o -> o.getOpcode() == Opcodes.NEW))
        return true;
    return false;
  }

  /** The setting {@code origins} names: the constant itself, or its key ({@code GlobalConfiguration.X.getKey()}). */
  private static String settingOf(final CallOperands.Method method, final Set<AbstractInsnNode> origins) {
    if (origins.size() == 1 && origins.iterator().next() instanceof MethodInsnNode call && SETTINGS.equals(call.owner) && "getKey".equals(
        call.name))
      return constantOf(method.stackBefore(call, 0));
    return constantOf(origins);
  }

  /** The {@code GlobalConfiguration} constant {@code origins} is, or null when it is anything else. */
  private static String constantOf(final Set<AbstractInsnNode> origins) {
    if (origins.size() == 1 && origins.iterator().next() instanceof FieldInsnNode field && field.getOpcode() == Opcodes.GETSTATIC
        && SETTINGS.equals(field.owner))
      return field.name;
    return null;
  }
}
