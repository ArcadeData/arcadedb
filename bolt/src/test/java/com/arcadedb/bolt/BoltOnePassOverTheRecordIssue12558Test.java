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
package com.arcadedb.bolt;

import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Bolt serializer reads a record ONCE, not once per property (issue #12558).
 * <p>
 * {@code BoltStructureMapper.toProperties} asked the document for each property by name, and
 * {@code ImmutableDocument.get(name)} walks the record header from its first byte until it meets the
 * name - so a record of P properties cost P walks of the same record, and the cost of a row grew with
 * P squared.
 * <p>
 * The document below counts the reads by name, so the test says how many the serializer asked for.
 */
class BoltOnePassOverTheRecordIssue12558Test extends BaseBoltServerTest {

  private static final int PROPERTIES = 40;

  /** A document that counts the reads by name, each a walk over the record header. */
  private static class CountingDocument extends ImmutableDocument {
    private int readsByName;

    private CountingDocument(final com.arcadedb.database.Database database, final DocumentType type, final RID rid) {
      super(database, type, rid, null);
    }

    @Override
    public Object get(final String propertyName) {
      ++readsByName;
      return super.get(propertyName);
    }

    private int readsByName() {
      return readsByName;
    }
  }

  @Test
  void thePropertiesOfARecordAreReadOnceNotOncePerProperty() {
    final var db = (com.arcadedb.database.DatabaseInternal) getServer(0).getDatabaseForOperator(getDatabaseName()).getEmbedded();
    db.transaction(() -> {
      db.getSchema().createDocumentType("Wide");
      final var type = db.getSchema().getType("Wide");
      final MutableDocument doc = db.newDocument("Wide");
      doc.set("nothing", null);
      for (int i = 0; i < PROPERTIES; i++)
        doc.set("p" + i, "value-" + i);
      final RID rid = doc.save().getIdentity();

      final CountingDocument document = new CountingDocument(db, type, rid);

      final var properties = com.arcadedb.bolt.structure.BoltStructureMapper.toProperties(document);

      assertThat(properties).hasSize(PROPERTIES + 1);
      assertThat(properties.keySet()).contains("nothing");
      for (int i = 0; i < PROPERTIES; i++)
        assertThat(properties).as("property p%d", i).containsEntry("p" + i, "value-" + i);
      assertThat(document.readsByName()).as("reads by name, each a walk over the record header").isZero();
    });
  }
}
