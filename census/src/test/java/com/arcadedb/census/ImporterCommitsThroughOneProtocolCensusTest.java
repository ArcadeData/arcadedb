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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11271: an importer row loop resolves the transaction it pushed through {@code ImporterTransactions}, the one protocol
 * that tells a commit refused before it starts (the transaction is still active and still the loop's, so it must be rolled
 * back) from one that popped it (what is on top is the caller's, and must be left alone). Issue #11208 converted the loops it
 * listed by hand, and {@code JsonlImporterFormat} was not on the list: its commit in a {@code finally} stranded a refused
 * transaction on the caller's thread.
 * <p>
 * The census ranges over the authority: every call of {@code commit()} on a {@code BasicDatabase} - the interface that declares
 * it - or anything that extends it, in the compiled classes the importer is made of ({@link CompiledClasses}), so an importer
 * format added later that commits by hand fails here and is sent to {@code ImporterTransactions.commitOwnTransaction}. What
 * remains is the protocol itself and the importer's own lifecycle, which commits a transaction no row loop holds.
 * <p>
 * The importer is what defines a row loop, not the package it is written in (issue #11388): every implementation of
 * {@code FormatImporter} and every subclass of {@code AbstractImporter}, in whichever module, and the classes of the integration
 * module's importer package ({@code GraphImporter} is an importer of its own, and neither of the two). The census used to keep
 * only the call sites keyed under {@code integration/importer/}, and the two formats of the gremlin module key as
 * {@code gremlin/integration/importer/}: {@code GraphSONImporterFormat} committed by hand, out of sight, and stranded a refused
 * commit exactly like #11208 and #11271.
 * <p>
 * The importer is also every class it hands its rows to, wherever that class is written (issue #11623): the census walks the
 * calls forward from the classes above ({@link CompiledClasses#methodsReachableFrom}) and keeps a commit of any class the walk
 * reaches, whichever of its methods holds it, because a delegate is often called back by a framework the walk cannot see into
 * (TinkerPop's readers commit through {@code ArcadeGraphTransaction#doCommit}). The helpers of a format used to count only when
 * they sat in the integration module's importer package, so a helper in another module was out of sight one level down from
 * where #11388 left the formats. The walk stops at the database: it does not step into the engine module, nor into an
 * implementation of {@code BasicDatabase}, whose own commits are the database's and not a row loop's.
 * <p>
 * What the walk cannot see is a class that no walked method constructs, calls or hands round as a method reference, and that
 * only an engine callback (a dispatch the walk does not follow) would reach, as when a container or a service lookup builds it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ImporterCommitsThroughOneProtocolCensusTest {
  private static final String IMPORTER          = "integration/importer/";
  private static final String IMPORTER_PACKAGE  = "com/arcadedb/" + IMPORTER;
  private static final String PROTOCOL          = IMPORTER + "ImporterTransactions#commitOwnTransaction";
  private static final String BASIC_DATABASE    = "com/arcadedb/database/BasicDatabase";
  private static final String FORMAT_IMPORTER   = "com/arcadedb/integration/importer/format/FormatImporter";
  private static final String ABSTRACT_IMPORTER = "com/arcadedb/integration/importer/AbstractImporter";
  /** A format outside the integration module, so outside the importer package spelling: the census must reach it. */
  private static final String GREMLIN_FORMAT    = "gremlin/integration/importer/format/GraphSONImporterFormat";
  /**
   * A commit of a class no importer package holds, in another module, reached only by walking the calls of a format: the census
   * must reach it, or it follows no delegate at all.
   */
  private static final String GREMLIN_DELEGATE  = "gremlin/ArcadeGraphTransaction#doCommit";

  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry(PROTOCOL, "the protocol itself"),
      Map.entry(IMPORTER + "AbstractImporter#openDatabase",
          "commits the schema the importer creates before any row loop starts, in a transaction it began on the line above"),
      Map.entry(IMPORTER + "AbstractImporter#closeDatabase",
          "closes a database the import itself created, after every row loop has returned"),
      Map.entry(IMPORTER + "Importer#closeDatabase", "closes a database the import itself created, after every row loop has returned"),
      Map.entry(IMPORTER + "Importer#load",
          "commits the import's ambient transaction once every format has returned, never the caller's (callerTransactionActiveOnEntry)"),
      Map.entry(GREMLIN_DELEGATE,
          "TinkerPop's readers commit their batches through graph.tx(): only on a graph the import owns, since in a caller's "
              + "transaction ArcadeGraph.openInCallerTransaction declares transactions unsupported and the readers never commit, "
              + "and the batch a refused commit leaves open is rolled back by AbstractTinkerPopImporterFormat.rollbackOwnBatch "
              + "when the import began it (Issue11388TinkerPopFormatsRefusedCommitTest)")));

  @Test
  void everyImporterRowLoopCommitsThroughImporterTransactions() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> databases = CompiledClasses.implementationsOf(BASIC_DATABASE, classes);
    final Set<String> importer = importerOf(classes, databases);

    assertThat(importer).as("the census must reach every FormatImporter implementation, also outside the integration module: "
        + GREMLIN_FORMAT).contains(GREMLIN_FORMAT);

    final Map<String, Set<String>> callSites = commitsOf(classes, importer, databases);

    assertThat(callSites).as("the census must reach the commit of the protocol it requires, " + PROTOCOL).containsKey(PROTOCOL);
    assertThat(callSites).as("the census must follow a format into the classes it delegates to, in any module: " + GREMLIN_DELEGATE)
        .containsKey(GREMLIN_DELEGATE);

    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as(
            "importer methods that commit a transaction by hand: a row loop resolves the transaction it pushed through "
                + "ImporterTransactions (beginOwnTransaction, commitOwnTransaction, rollbackOwnTransaction), or a refused commit "
                + "strands it on the caller's thread (issues #11208, #11271, #11388, #11623)")
        .isEmpty();
    assertThat(CompiledClasses.stale(callSites, REVIEWED)).as("REVIEWED entries that no longer commit: remove them").isEmpty();
  }

  /**
   * The control (#11623): a format outside every importer package that hands its rows to helpers calling {@code commit()} by
   * hand is caught in each, whichever way it reaches them: directly, as a method reference, or through the interface or the
   * abstract base the helper implements. A class that commits but no importer reaches is not caught, nor is an implementation
   * of the database.
   */
  @Test
  void theCensusFollowsAFormatIntoEveryHelperItDelegatesTo() throws Exception {
    final Map<String, byte[]> classes = new HashMap<>(CompiledClasses.ofEveryModule());
    classes.putAll(CompiledClasses.ofTheProbes());
    final Set<String> databases = CompiledClasses.implementationsOf(BASIC_DATABASE, classes);

    final Map<String, Set<String>> callSites = commitsOf(classes, importerOf(classes, databases), databases);

    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as("the probes of org.acme.loader.DelegatingFormatProbe")
        .containsExactlyInAnyOrder("org/acme/loader/HandRolledRowLoopProbe#run (Database.commit)",
            "org/acme/loader/MethodReferencedRowLoopProbe#run (Database.commit)",
            "org/acme/loader/CommittingRowSinkProbe#accept (BasicDatabase.commit)",
            "org/acme/loader/CommittingRowSourceProbe#drain (Database.commit)",
            "org/acme/loader/InheritedSinkBodyProbe#accept (BasicDatabase.commit)",
            "org/acme/loader/InheritedLoopBodyProbe#run (Database.commit)");
    assertThat(callSites).as("a class no importer reaches is not an importer's").doesNotContainKey("org/acme/loader/UnreachedCommitProbe#run");
    assertThat(callSites).as("the walk stops at an implementation of the database, whose commits are the database's")
        .doesNotContainKey("org/acme/loader/DatabaseImplementationProbe#flush");
  }

  /**
   * The classes the importer is made of, keyed as {@link CompiledClasses} keys them: its formats, its importers and the classes
   * of its package, and every class the walk of their calls reaches short of the database ({@code databases} and the engine).
   */
  private static Set<String> importerOf(final Map<String, byte[]> classes, final Set<String> databases)
      throws IOException, URISyntaxException {
    final Set<String> engine = new HashSet<>();
    for (final String classFile : CompiledClasses.byModule().get("engine").keySet())
      engine.add(internalNameOf(classFile));

    final Set<String> roots = new HashSet<>(CompiledClasses.implementationsOf(FORMAT_IMPORTER, classes));
    roots.addAll(CompiledClasses.implementationsOf(ABSTRACT_IMPORTER, classes));
    for (final String classFile : classes.keySet())
      if (classFile.startsWith(IMPORTER_PACKAGE))
        roots.add(internalNameOf(classFile));

    final Set<String> importer = new TreeSet<>();
    for (final String method : CompiledClasses.methodsReachableFrom(classes, roots,
        type -> !engine.contains(type) && !databases.contains(type)))
      importer.add(CompiledClasses.keyOf(CompiledClasses.typeOf(method) + ".class"));
    return importer;
  }

  /** Every call of {@code commit()} on a database ({@code databases}) in a class of {@code importer}. */
  private static Map<String, Set<String>> commitsOf(final Map<String, byte[]> classes, final Set<String> importer,
      final Set<String> databases) {
    return CompiledClasses.callSitesOf(classes,
            (owner, name, desc) -> databases.contains(owner) && "commit".equals(name) && "()V".equals(desc))//
        .entrySet().stream().filter(site -> importer.contains(classOf(site.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));
  }

  /** {@code a/C.class} -> {@code a/C}. */
  private static String internalNameOf(final String classFile) {
    return classFile.substring(0, classFile.length() - ".class".length());
  }

  /** {@code a/C#m} -> {@code a/C}. */
  private static String classOf(final String callSite) {
    return callSite.substring(0, callSite.indexOf('#'));
  }
}
