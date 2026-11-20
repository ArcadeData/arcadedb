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
package org.acme.loader;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.integration.importer.format.FormatImporter;

import java.util.function.Consumer;

/**
 * Never run: compiled only so the control of {@code ImporterCommitsThroughOneProtocolCensusTest} can show that a format
 * handing its rows to helpers outside any importer package, in a module of its own, is followed into them (#11623). The
 * census once kept a helper's commit only when the helper sat in the integration module's importer package, and every class
 * here was invisible to it.
 * <p>
 * Every helper is reached by one way of calling and no other, so that taking that way out of the walk
 * ({@code CompiledClasses#methodsReachableFrom}) leaves its class unreached and the control red: a census scope is per
 * class, and a helper the format also constructed itself would be in scope whatever the walk did with the call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DelegatingFormatProbe implements FormatImporter {
  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser,
      final DatabaseInternal database, final ImporterContext context, final ImporterSettings settings) {
    // CALLED DIRECTLY
    HandRolledRowLoopProbe.run(database);

    // HANDED ROUND AS A METHOD REFERENCE
    final Consumer<Database> loop = MethodReferencedRowLoopProbe::run;
    loop.accept(database);

    // REACHED ONLY BY DISPATCH, THROUGH THE INTERFACE AND THROUGH THE ABSTRACT BASE THE CLASS EXTENDS: THE REGISTRY HOLDS THEM
    // WHERE NO WALKED METHOD CREATES THEM, AS A CONTAINER OR A SERVICE LOOKUP WOULD
    UnwalkedRegistryProbe.SINK.accept(database);
    UnwalkedRegistryProbe.SOURCE.drain(database);

    // THE SAME WITH A BODY THE CLASS INHERITS RATHER THAN OVERRIDES: BY DISPATCH FROM THE INTERFACE, AND BY A CALL ON THE CLASS
    // WHOSE INTERFACE DECLARES THE METHOD ABSTRACTLY WHILE ITS SUPER CLASS HOLDS THE BODY
    UnwalkedRegistryProbe.INHERITING_SINK.accept(database);
    UnwalkedRegistryProbe.INHERITING_LOOP.run(database);

    // AN IMPLEMENTATION OF THE DATABASE: THE WALK DOES NOT STEP INTO IT
    DatabaseImplementationProbe.flush(database);
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) {
    return null;
  }

  @Override
  public String getFormat() {
    return "probe";
  }
}

/** The shape of #11623: the format's row loop, written in a helper that is neither a format nor an importer. */
final class HandRolledRowLoopProbe {
  private HandRolledRowLoopProbe() {
  }

  static void run(final Database database) {
    database.begin();
    database.commit();
  }
}

/** A row loop a format hands round as a method reference, as it does with a reader factory: the walk follows the handle. */
final class MethodReferencedRowLoopProbe {
  private MethodReferencedRowLoopProbe() {
  }

  static void run(final Database database) {
    database.commit();
  }
}

/** What a format hands its rows to, as an interface: the walk must follow the call into its implementation. */
interface RowSinkProbe {
  void accept(BasicDatabase database);
}

/** Commits on a {@link BasicDatabase}, the interface that declares {@code commit()}, which the census once did not match. */
final class CommittingRowSinkProbe implements RowSinkProbe {
  @Override
  public void accept(final BasicDatabase database) {
    database.commit();
  }
}

/** What a format hands its rows to, as an abstract base: the walk must follow the call into its override. */
abstract class RowSourceProbe {
  abstract void drain(Database database);
}

/** Commits by hand in the override of the abstract base. */
final class CommittingRowSourceProbe extends RowSourceProbe {
  @Override
  void drain(final Database database) {
    database.commit();
  }
}

/** The body {@link InheritingSinkProbe} runs: its super class holds it, and the interface it implements declares none. */
class InheritedSinkBodyProbe {
  public void accept(final BasicDatabase database) {
    database.commit();
  }
}

/** Implements {@link RowSinkProbe} by inheriting {@code accept} from its super class, which does not implement it. */
final class InheritingSinkProbe extends InheritedSinkBodyProbe implements RowSinkProbe {
}

/** What a format calls on the class itself, as an interface: the class gets its body from its super class. */
interface RowLoopProbe {
  void run(Database database);
}

/** The body {@link InheritingLoopProbe} runs, declared once and only here. */
class InheritedLoopBodyProbe {
  public void run(final Database database) {
    database.commit();
  }
}

/** Implements {@link RowLoopProbe} by inheriting {@code run} from its super class: a call on this class finds the body there. */
final class InheritingLoopProbe extends InheritedLoopBodyProbe implements RowLoopProbe {
}

/** Creates the helpers that only a dispatch may reach: no call of a walked method runs this initializer. */
final class UnwalkedRegistryProbe {
  static final RowSinkProbe        SINK            = new CommittingRowSinkProbe();
  static final RowSourceProbe      SOURCE          = new CommittingRowSourceProbe();
  static final RowSinkProbe        INHERITING_SINK = new InheritingSinkProbe();
  static final InheritingLoopProbe INHERITING_LOOP = new InheritingLoopProbe();

  private UnwalkedRegistryProbe() {
  }
}

/** An implementation of the database: its commits are the database's, and the walk must not descend into it. */
abstract class DatabaseImplementationProbe implements BasicDatabase {
  static void flush(final BasicDatabase database) {
    database.commit();
  }
}

/** Commits by hand, but no importer reaches it: the census must not take every commit for an importer's. */
final class UnreachedCommitProbe {
  private UnreachedCommitProbe() {
  }

  static void run(final Database database) {
    database.commit();
  }
}
