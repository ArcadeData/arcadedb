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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExecutionPlanCacheTest {

  @Test
  public void testCacheInvalidation1() throws InterruptedException {
    String testName = "testCacheInvalidation1";

    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory("ExecutionPlanCacheTest").create();
    try {
      db.begin();
      db.getSchema().createDocumentType("OUser");
      db.newDocument("OUser").set("name", "jay").save();
      db.commit();

      ExecutionPlanCache cache = ExecutionPlanCache.instance(db);
      String stm = "SELECT FROM OUser";

      /*
       * the cache has a mechanism that guarantees that if you are doing execution planning
       * and the cache is invalidated in the meantime, the newly generated execution plan
       * is not cached. This mechanism relies on a System.currentTimeMillis(), so it can happen
       * that the execution planning is done right after the cache invalidation, but still in THE SAME
       * millisecond, this Thread.sleep() guarantees that the new execution plan is generated
       * at least one ms after last invalidation, so it is cached.
       */
      Thread.sleep(2);

      // schema changes
      db.query("sql", stm).close();
      cache = ExecutionPlanCache.instance(db);
      Assertions.assertTrue(cache.contains(stm));

      DocumentType clazz = db.getSchema().createDocumentType(testName);
      Assertions.assertFalse(cache.contains(stm));

      Thread.sleep(2);

      // schema changes 2
      db.query("sql", stm).close();
      cache = ExecutionPlanCache.instance(db);
      Assertions.assertTrue(cache.contains(stm));

      Property prop = clazz.createProperty("name", Type.STRING);
      Assertions.assertFalse(cache.contains(stm));

      Thread.sleep(2);

      // index changes
      db.query("sql", stm).close();
      cache = ExecutionPlanCache.instance(db);
      Assertions.assertTrue(cache.contains(stm));

      db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, testName, "name");
      Assertions.assertFalse(cache.contains(stm));

    } finally {
      db.drop();
      Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    }
  }
}
