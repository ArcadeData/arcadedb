/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #9175: SQL has no PAGESIZE clause for CREATE INDEX, so the default page size of a new LSM index
 * (a transaction copies a whole page for every page it changes) has to be tunable through a setting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IndexDefaultPageSizeTest extends TestHelper {

  @Test
  void defaultStaysTheLsmDefault() {
    database.command("sql", "CREATE DOCUMENT TYPE DefaultSized");
    database.command("sql", "CREATE PROPERTY DefaultSized.k LONG");
    database.command("sql", "CREATE INDEX ON DefaultSized (k) UNIQUE");
    assertThat(pageSizeOf("DefaultSized")).isEqualTo(LSMTreeIndexAbstract.DEF_PAGE_SIZE);
  }

  @Test
  void settingChangesThePageSizeOfNewIndexesOnly() {
    database.command("sql", "CREATE DOCUMENT TYPE OldIdx");
    database.command("sql", "CREATE PROPERTY OldIdx.k LONG");
    database.command("sql", "CREATE INDEX ON OldIdx (k) UNIQUE");

    database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE, 32_768);
    try {
      database.command("sql", "CREATE DOCUMENT TYPE NewIdx");
      database.command("sql", "CREATE PROPERTY NewIdx.k LONG");
      database.command("sql", "CREATE INDEX ON NewIdx (k) UNIQUE");

      assertThat(pageSizeOf("NewIdx")).isEqualTo(32_768);
      assertThat(pageSizeOf("OldIdx")).isEqualTo(LSMTreeIndexAbstract.DEF_PAGE_SIZE);

      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++)
          database.newDocument("NewIdx").set("k", (long) i).save();
      });
      assertThat(database.query("sql", "SELECT FROM NewIdx WHERE k = 1500").stream().count()).isEqualTo(1L);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE,
          GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.getDefValue());
    }
  }

  @Test
  void tooSmallValueIsRaisedToTheMinimum() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE, 100);
    try {
      database.command("sql", "CREATE DOCUMENT TYPE Tiny");
      database.command("sql", "CREATE PROPERTY Tiny.k LONG");
      database.command("sql", "CREATE INDEX ON Tiny (k) UNIQUE");
      assertThat(pageSizeOf("Tiny")).isEqualTo(8_192);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE,
          GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.getDefValue());
    }
  }

  @Test
  void settingDoesNotChangeFullTextIndexes() {
    database.command("sql", "CREATE DOCUMENT TYPE Articles");
    database.command("sql", "CREATE PROPERTY Articles.body STRING");
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE, 8_192);
    try {
      database.command("sql", "CREATE INDEX ON Articles (body) FULL_TEXT");
      database.transaction(() -> database.newDocument("Articles").set("body", "hello page size world").save());
      assertThat(database.query("sql", "SELECT FROM Articles WHERE SEARCH_INDEX('Articles[body]', 'hello') = true").stream().count())
          .isEqualTo(1L);
      assertThat(pageSizeOf("Articles")).isEqualTo(LSMTreeIndexAbstract.DEF_PAGE_SIZE);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE,
          GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.getDefValue());
    }
  }

  @Test
  void explicitPageSizeBeatsTheSetting() {
    database.getSchema().createDocumentType("Explicit").createProperty("k", Type.LONG);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE, 32_768);
    try {
      database.getSchema().buildTypeIndex("Explicit", new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(65_536).create();
      assertThat(pageSizeOf("Explicit")).isEqualTo(65_536);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE,
          GlobalConfiguration.INDEX_DEFAULT_PAGE_SIZE.getDefValue());
    }
  }

  private int pageSizeOf(final String type) {
    return database.getSchema().getType(type).getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0].getPageSize();
  }
}
