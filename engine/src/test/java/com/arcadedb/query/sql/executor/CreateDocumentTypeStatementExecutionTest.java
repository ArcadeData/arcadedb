package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreateDocumentTypeStatementExecutionTest {
  @Test
  public void testPlain() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = "testPlain";
      ResultSet result = db.command("sql", "create document type " + className);
      Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      result.close();
    });
  }

  @Test
  public void testClusters() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = "testClusters";
      ResultSet result = db.command("sql", "create document type " + className + " buckets 32");
      Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      Assertions.assertEquals(32, clazz.getBuckets(false).size());
      result.close();
    });
  }

  @Test
  public void testIfNotExists() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = "testIfNotExists";
      ResultSet result = db.command("sql", "create document type " + className + " if not exists");
      Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      result.close();

      result = db.command("sql", "create document type " + className + " if not exists");
      clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      result.close();
    });
  }
}
