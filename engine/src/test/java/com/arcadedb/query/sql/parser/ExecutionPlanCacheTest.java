package com.arcadedb.query.sql.parser;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.*;
import org.junit.jupiter.api.Assertions;

public class ExecutionPlanCacheTest {

  //@Test
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
    }
  }
}
