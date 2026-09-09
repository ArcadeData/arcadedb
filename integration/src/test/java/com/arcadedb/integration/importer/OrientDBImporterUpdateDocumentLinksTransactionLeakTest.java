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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7272 (finding 3): {@code OrientDBImporter.updateDocumentLinks()} opens a transaction, loops over
 * {@code documentsWithLinksToUpdate}, and only commits at the very end - no {@code finally}. A failure
 * mid-loop (here: {@code compressedRecordsRidMap} not yet populated, the state {@code run()} would be in
 * before {@code createIndexes()}/schema-analysis errors surface) leaves that transaction on the stack.
 * <p>
 * {@code updateDocumentLinks()} is private and depends only on instance fields already reachable through
 * the public embedding constructor, so it is invoked here via reflection rather than through the whole
 * {@code run()} pipeline (which needs a full gzipped OrientDB export to reach this code path at all).
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class OrientDBImporterUpdateDocumentLinksTransactionLeakTest {

  private static final String DB_PATH = "target/databases/orientdb-importer-updatelinks-tx-leak-test";

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person");
      database.getSchema().createDocumentType("Marker");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @SuppressWarnings("unchecked")
  private void seedOneDocumentWithLinksToUpdate(final OrientDBImporter importer) throws Exception {
    database.begin();
    final RID personRid = database.newDocument("Person").set("link", new RID(9999, 9999)).save().getIdentity();
    database.commit();

    final Field field = OrientDBImporter.class.getDeclaredField("documentsWithLinksToUpdate");
    field.setAccessible(true);
    ((Set<RID>) field.get(importer)).add(personRid);
  }

  private void invokeUpdateDocumentLinks(final OrientDBImporter importer) throws Throwable {
    final Method method = OrientDBImporter.class.getDeclaredMethod("updateDocumentLinks");
    method.setAccessible(true);
    try {
      method.invoke(importer);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private long countOf(final String typeName) {
    return database.query("sql", "select count(*) as c from " + typeName).next().<Long>getProperty("c");
  }

  @Test
  void aFailedLinkUpdateLeavesNoTransactionActive() throws Exception {
    final OrientDBImporter importer = new OrientDBImporter((DatabaseInternal) database, new ImporterSettings());
    seedOneDocumentWithLinksToUpdate(importer);

    // compressedRecordsRidMap is only populated inside run(); left null here, resolving the LINK property
    // throws a NullPointerException - the kind of mid-loop failure the issue is about.
    assertThatThrownBy(() -> invokeUpdateDocumentLinks(importer)).isInstanceOf(NullPointerException.class);

    assertThat(database.isTransactionActive())
        .as("the transaction updateDocumentLinks() opened must be resolved before the failure propagates")
        .isFalse();
  }

  @Test
  void aFailedLinkUpdateDoesNotShadowTheCallersOwnTransaction() throws Exception {
    final OrientDBImporter importer = new OrientDBImporter((DatabaseInternal) database, new ImporterSettings());
    seedOneDocumentWithLinksToUpdate(importer);

    database.begin();
    database.newDocument("Marker").set("name", "caller").save();

    assertThatThrownBy(() -> invokeUpdateDocumentLinks(importer)).isInstanceOf(NullPointerException.class);

    database.commit();

    assertThat(database.isTransactionActive())
        .as("one commit for the one transaction the caller opened must leave nothing active")
        .isFalse();
    assertThat(countOf("Marker"))
        .as("the caller's own record must be what its commit made durable")
        .isEqualTo(1);
  }
}
