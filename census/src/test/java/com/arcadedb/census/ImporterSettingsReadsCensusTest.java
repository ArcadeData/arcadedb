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
import com.arcadedb.integration.importer.ImporterSettings;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11311: {@code ImporterSettings.parseParameter} refuses a setting that is not in {@link ImporterSettings#SETTINGS}, so a
 * format that reads a setting missing from that set makes it impossible to pass, and an entry nothing reads is a typo accepted
 * again. The census ranges over the authority: every read of a setting by name in the compiled classes of every module, through
 * the accessors of {@code ImporterSettings} or its {@code options} map, and the branches of {@code parseParameter} itself. A read
 * is keyed by what produced the name ({@link CallOperands}), so one whose name is not a constant has no name to check and fails.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ImporterSettingsReadsCensusTest {
  private static final String SETTINGS_CLASS = "com/arcadedb/integration/importer/ImporterSettings";
  private static final Set<String> ACCESSORS = Set.of("getValue", "getIntValue", "getLongValue", "getPropertyNameValue");
  private static final Set<String> MAP_READS = Set.of("get", "containsKey", "getOrDefault");

  /** Reads by a name that is not a constant, each with where the name comes from. */
  private static final Map<String, String> NOT_A_CONSTANT_NAME = new TreeMap<>(Map.of(//
      "integration/importer/OrientDBImporter#createType", "the key of GlobalConfiguration.TYPE_DEFAULT_BUCKETS, added to `handled` below",//
      "integration/importer/ImporterSettings#getValue", "the accessor itself, reading the name it was handed",//
      "integration/importer/ImporterSettings#getIntValue", "the accessor itself, reading the name it was handed",//
      "integration/importer/ImporterSettings#getLongValue", "the accessor itself, reading the name it was handed",//
      "integration/importer/ImporterSettings#getPropertyNameValue", "the accessor itself, reading the name it was handed"));

  @Test
  void everySettingReadByNameIsOneTheParserAcceptsAndEveryAcceptedOneIsRead() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final List<CallOperands.Site> calls = CallOperands.of(classes, (owner, name, desc) ->
        (SETTINGS_CLASS.equals(owner) && ACCESSORS.contains(name)) || ("java/util/Map".equals(owner) && MAP_READS.contains(name)));

    final Set<String> read = new TreeSet<>();
    final Map<String, Set<String>> withoutAConstantName = new TreeMap<>();
    for (final CallOperands.Site site : calls) {
      if (!SETTINGS_CLASS.equals(site.call().owner) && !isTheOptionsMap(site))
        continue;

      final String name = CallOperands.stringConstant(site.argument(0));
      if (name != null)
        read.add(name);
      else
        withoutAConstantName.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(site.call().name);
    }

    assertThat(read).as("the census must reach the reads of a setting by name, here objectNestLevel in XMLImporterFormat")
        .contains("objectNestLevel");
    assertThat(read).as("the census must reach the reads off the options map, here fileType in SourceDiscovery").contains("fileType");

    assertThat(CompiledClasses.unreviewed(withoutAConstantName, NOT_A_CONSTANT_NAME)).as(
        "a setting read by a name that is not a constant cannot be checked against ImporterSettings.SETTINGS").isEmpty();
    assertThat(CompiledClasses.stale(withoutAConstantName, NOT_A_CONSTANT_NAME)).as("entries that no longer read: remove them")
        .isEmpty();

    assertThat(read).as("settings read by name that ImporterSettings.SETTINGS refuses: nobody could pass them (issue #11311)")
        .isSubsetOf(ImporterSettings.SETTINGS);

    final Set<String> handled = new TreeSet<>(read);
    handled.addAll(branchesOfParseParameter(classes));
    handled.add(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey());
    assertThat(handled).as("the census must reach the branches of parseParameter, here commitEvery").contains("commitEvery");

    final Set<String> neverRead = new TreeSet<>(ImporterSettings.SETTINGS);
    neverRead.removeAll(handled);
    assertThat(neverRead).as("settings ImporterSettings.SETTINGS accepts and nothing reads: an accepted typo (issue #11311)").isEmpty();
  }

  /** Whether the receiver of the call is the {@code options} field of {@code ImporterSettings}. */
  private static boolean isTheOptionsMap(final CallOperands.Site site) {
    for (final AbstractInsnNode origin : site.operand(0))
      if (origin instanceof FieldInsnNode field && SETTINGS_CLASS.equals(field.owner) && "options".equals(field.name))
        return true;
    return false;
  }

  /** The constants {@code parseParameter} switches on: every string it loads. */
  private static Set<String> branchesOfParseParameter(final Map<String, byte[]> classes) {
    final byte[] bytes = classes.entrySet().stream().filter(e -> e.getKey().endsWith("importer/ImporterSettings.class")).findFirst()
        .orElseThrow(() -> new AssertionError("the census must read ImporterSettings")).getValue();
    final ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, ClassReader.SKIP_FRAMES);

    final Set<String> constants = new TreeSet<>();
    for (final MethodNode method : node.methods)
      if ("parseParameter".equals(method.name))
        for (final AbstractInsnNode instruction : method.instructions)
          if (instruction instanceof LdcInsnNode ldc && ldc.cst instanceof String constant)
            constants.add(constant);
    return constants;
  }
}
