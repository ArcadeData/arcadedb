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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7701: handing the importer a database it cannot load into says so, and says what to do instead.
 * <p>
 * Every format writes through {@code DatabaseInternal} - the TIMESERIES sample path through
 * {@code LocalTimeSeriesType.getEngine()}, and the document, vertex and edge paths through the database itself -
 * and {@code AbstractImporter} opens its own target with a local {@code DatabaseFactory}. So the importer is
 * embedded-only, for every record type rather than only for samples.
 * <p>
 * That is not a gap in what a remote client can do: {@code IMPORT DATABASE '<url>'} is a SQL statement, which a
 * remote connection issues like any other command and the server runs against its own embedded instance
 * ({@code Issue7701RemoteTimeSeriesLogicalRestoreIT} drives that whole round trip). The refusal has to NAME that,
 * because a bare {@code ClassCastException} - which is what the unchecked cast here used to raise - names a class
 * the caller never mentioned and no alternative at all.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7701">issue #7701</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7701ImporterRefusesANonEmbeddedDatabaseTest {

  /**
   * A {@link Database} that is not a {@code DatabaseInternal}. A proxy rather than a hand-written stub: the
   * interface has far too many methods to implement for a constructor argument that is never called, and the
   * constructor refuses this before anything is invoked on it.
   */
  private static Database notEmbedded() {
    return (Database) Proxy.newProxyInstance(Database.class.getClassLoader(), new Class<?>[] { Database.class },
        (proxy, method, args) -> {
          throw new AssertionError("The importer must refuse this database before calling " + method.getName());
        });
  }

  @Test
  void aNonEmbeddedDatabaseIsRefusedWithTheSupportedAlternative() {
    assertThatThrownBy(() -> new Importer(notEmbedded(), "file://somewhere.jsonl.tgz"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("embedded database")
        .as("a refusal that does not say what to do instead is the ClassCastException again, with words")
        .hasMessageContaining("IMPORT DATABASE");
  }

  /**
   * {@code null} still passes through, exactly as the unchecked cast it replaced did: it means "no database yet",
   * and {@code AbstractImporter.openDatabase()} then opens one from the settings. Pinned because turning the cast
   * into a check is precisely where that would have been lost - and was, until
   * {@code Issue6474ImportSsrfFlagDivergenceTest} caught it.
   */
  @Test
  void aNullDatabaseStillMeansOpenOneFromTheSettings() {
    final Importer importer = new Importer(null, "file://somewhere.jsonl.tgz");

    assertThat(importer).isNotNull();
    assertThat(importer.getContext()).isNotNull();
  }
}
