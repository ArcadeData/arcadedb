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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11271: an importer row loop resolves the transaction it pushed through {@code ImporterTransactions}, the one protocol
 * that tells a commit refused before it starts (the transaction is still active and still the loop's, so it must be rolled
 * back) from one that popped it (what is on top is the caller's, and must be left alone). Issue #11208 converted the loops it
 * listed by hand, and {@code JsonlImporterFormat} was not on the list: its commit in a {@code finally} stranded a refused
 * transaction on the caller's thread.
 * <p>
 * The census ranges over the authority: every call of {@code commit()} on a {@code Database} - or anything that extends it - in
 * the compiled classes of the importer ({@link CompiledClasses}), so an importer format added later that commits by hand fails
 * here and is sent to {@code ImporterTransactions.commitOwnTransaction}. What remains is the protocol itself and the importer's
 * own lifecycle, which commits a transaction no row loop holds.
 * <p>
 * The importer is what defines a row loop, not the package it is written in (issue #11388): every implementation of
 * {@code FormatImporter} and every subclass of {@code AbstractImporter}, in whichever module, plus the classes of the
 * integration module's importer package a format delegates its rows to ({@code Neo4jImporter}, {@code GraphImporter}, ...). The
 * census used to keep only the call sites keyed under {@code integration/importer/}, and the two formats of the gremlin module key
 * as {@code gremlin/integration/importer/}: {@code GraphSONImporterFormat} committed by hand, out of sight, and stranded a
 * refused commit exactly like #11208 and #11271.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ImporterCommitsThroughOneProtocolCensusTest {
  private static final String IMPORTER          = "integration/importer/";
  private static final String PROTOCOL          = IMPORTER + "ImporterTransactions#commitOwnTransaction";
  private static final String FORMAT_IMPORTER   = "com/arcadedb/integration/importer/format/FormatImporter";
  private static final String ABSTRACT_IMPORTER = "com/arcadedb/integration/importer/AbstractImporter";
  /** A format outside the integration module, so outside the importer package spelling: the census must reach it. */
  private static final String GREMLIN_FORMAT    = "gremlin/integration/importer/format/GraphSONImporterFormat";

  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry(PROTOCOL, "the protocol itself"),
      Map.entry(IMPORTER + "AbstractImporter#openDatabase",
          "commits the schema the importer creates before any row loop starts, in a transaction it began on the line above"),
      Map.entry(IMPORTER + "AbstractImporter#closeDatabase",
          "closes a database the import itself created, after every row loop has returned"),
      Map.entry(IMPORTER + "Importer#closeDatabase", "closes a database the import itself created, after every row loop has returned"),
      Map.entry(IMPORTER + "Importer#load",
          "commits the import's ambient transaction once every format has returned, never the caller's (callerTransactionActiveOnEntry)")));

  @Test
  void everyImporterRowLoopCommitsThroughImporterTransactions() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> databases = CompiledClasses.implementationsOf("com/arcadedb/database/Database", classes);
    final Set<String> importer = new HashSet<>();
    for (final String type : CompiledClasses.implementationsOf(FORMAT_IMPORTER, classes))
      importer.add(CompiledClasses.keyOf(type));
    for (final String type : CompiledClasses.implementationsOf(ABSTRACT_IMPORTER, classes))
      importer.add(CompiledClasses.keyOf(type));

    assertThat(importer).as("the census must reach every FormatImporter implementation, also outside the integration module: "
        + GREMLIN_FORMAT).contains(GREMLIN_FORMAT);

    final Map<String, Set<String>> callSites = CompiledClasses.callSitesOf(classes,
            (owner, name, desc) -> databases.contains(owner) && "commit".equals(name) && "()V".equals(desc))//
        .entrySet().stream().filter(site -> site.getKey().startsWith(IMPORTER) || importer.contains(classOf(site.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));

    assertThat(callSites).as("the census must reach the commit of the protocol it requires, " + PROTOCOL).containsKey(PROTOCOL);

    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as(
            "importer methods that commit a transaction by hand: a row loop resolves the transaction it pushed through "
                + "ImporterTransactions (beginOwnTransaction, commitOwnTransaction, rollbackOwnTransaction), or a refused commit "
                + "strands it on the caller's thread (issues #11208, #11271, #11388)")
        .isEmpty();
    assertThat(CompiledClasses.stale(callSites, REVIEWED)).as("REVIEWED entries that no longer commit: remove them").isEmpty();
  }

  /** {@code a/C#m} -> {@code a/C}. */
  private static String classOf(final String callSite) {
    return callSite.substring(0, callSite.indexOf('#'));
  }
}
