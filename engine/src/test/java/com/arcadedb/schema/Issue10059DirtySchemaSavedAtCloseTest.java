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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10059: a schema change still pending when the database closes (a DDL inside an open
 * transaction postpones its save) was lost as soon as the schema held an index. The close-time save ran once the database
 * was already marked closed, and serializing an index resolves its bucket through the checked schema accessor, which
 * threw {@code DatabaseIsClosedException}: the save was logged and dropped, the close succeeded.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10059DirtySchemaSavedAtCloseTest extends TestHelper {

  @Test
  void typeAndIndexCreatedInAnOpenTransactionSurviveTheClose() {
    database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
    database.getSchema().getType("Doc").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    database.begin();
    // A DDL INSIDE AN OPEN TRANSACTION POSTPONES ITS SCHEMA SAVE: THE SCHEMA IS DIRTY AT CLOSE
    database.getSchema().createDocumentType("Late").createProperty("k", Type.STRING);
    database.getSchema().getType("Late").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
    assertThat(((LocalSchema) database.getSchema()).isDirty()).isTrue();

    reopenDatabase();

    assertThat(database.getSchema().existsType("Late")).isTrue();
    assertThat(database.getSchema().getType("Late").existsProperty("k")).isTrue();
    final Index index = database.getSchema().getType("Late").getPolymorphicIndexByProperties("k");
    assertThat(index).isNotNull();
    assertThat(((LocalSchema) database.getSchema()).isDirty()).isFalse();

    // THE INDEX IS USABLE ACROSS THE REOPEN, NOT ONLY NAMED BY THE SCHEMA
    database.transaction(() -> database.newDocument("Late").set("k", "v").save());
    assertThat(database.query("sql", "SELECT FROM Late WHERE k = 'v'").stream().count()).isEqualTo(1);
  }

  @Test
  void fullTextIndexCreatedInAnOpenTransactionSurvivesTheClose() {
    database.begin();
    database.getSchema().createDocumentType("Article").createProperty("body", Type.STRING);
    database.getSchema().getType("Article").createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "body");
    assertThat(((LocalSchema) database.getSchema()).isDirty()).isTrue();

    reopenDatabase();

    assertThat(database.getSchema().existsType("Article")).isTrue();
    assertThat(database.getSchema().getType("Article").getPolymorphicIndexByProperties("body")).isNotNull();
  }
}
