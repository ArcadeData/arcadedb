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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionPlanCacheTest {

  @Test
  void cacheInvalidation1() throws Exception {
    final String testName = "testCacheInvalidation1";

    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory("ExecutionPlanCacheTest").create();
    try {
      db.begin();
      db.getSchema().createDocumentType("OUser");
      db.newDocument("OUser").set("name", "jay").save();
      db.commit();

      ExecutionPlanCache cache = ExecutionPlanCache.instance(db);
      final String stm = "SELECT FROM OUser";

      // NO PAUSE BETWEEN AN INVALIDATION AND THE NEXT PLANNING: THE CACHE COUNTS INVALIDATIONS INSTEAD OF TIMESTAMPING
      // THEM, SO A PLAN BUILT IN THE SAME MILLISECOND AS THE SCHEMA CHANGE BEFORE IT IS CACHED TOO (#8635)
      // schema changes
      db.query("sql", stm).close();
      cache = ExecutionPlanCache.instance(db);
      assertThat(cache.contains(stm)).isTrue();

      final DocumentType clazz = db.getSchema().createDocumentType(testName);
      assertThat(cache.contains(stm)).isFalse();

      // schema changes 2
      db.query("sql", stm).close();
      cache = ExecutionPlanCache.instance(db);
      assertThat(cache.contains(stm)).isTrue();

      final Property prop = clazz.createProperty("name", Type.STRING);
      assertThat(cache.contains(stm)).isFalse();

      // index changes
      db.query("sql", stm).close();
      cache = ExecutionPlanCache.instance(db);
      assertThat(cache.contains(stm)).isTrue();

      db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, testName, "name");
      assertThat(cache.contains(stm)).isFalse();

    } finally {
      db.drop();
      TestHelper.checkActiveDatabases();
    }
  }
}
