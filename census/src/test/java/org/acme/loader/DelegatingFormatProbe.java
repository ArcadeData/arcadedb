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

/**
 * Never run: compiled only so the control of {@code ImporterCommitsThroughOneProtocolCensusTest} can show that a format
 * handing its rows to helpers outside any importer package, in a module of its own, is followed into them (#11623). The
 * census once kept a helper's commit only when the helper sat in the integration module's importer package, and every class
 * here was invisible to it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DelegatingFormatProbe implements FormatImporter {
  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser,
      final DatabaseInternal database, final ImporterContext context, final ImporterSettings settings) {
    // A HELPER CALLED DIRECTLY, AND ONE REACHED ONLY THROUGH THE INTERFACE IT IMPLEMENTS
    HandRolledRowLoopProbe.run(database);
    final RowSinkProbe sink = new CommittingRowSinkProbe();
    sink.accept(database);
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

/** Commits by hand, but no importer reaches it: the census must not take every commit for an importer's. */
final class UnreachedCommitProbe {
  private UnreachedCommitProbe() {
  }

  static void run(final Database database) {
    database.commit();
  }
}
