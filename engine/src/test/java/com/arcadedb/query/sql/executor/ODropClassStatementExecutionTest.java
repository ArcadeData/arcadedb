package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODropClassStatementExecutionTest extends TestHelper {
  @Test
  public void testPlain() {
    String className = "testPlain";
    Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    ResultSet result = database.command("sql", "drop type " + className);
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertEquals("drop type", next.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    Assertions.assertFalse(schema.existsType(className));
  }

  @Test
  public void testIfExists() {
    String className = "testIfExists";
    Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    ResultSet result = database.command("sql", "drop type " + className + " if exists");
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertEquals("drop type", next.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    Assertions.assertFalse(schema.existsType(className));

    result = database.command("sql", "drop type " + className + " if exists");
    result.close();

    Assertions.assertFalse(schema.existsType(className));
  }

  @Test
  public void testParam() {
    String className = "testParam";
    Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    ResultSet result = database.command("sql", "drop type ?", className);
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertEquals("drop type", next.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    Assertions.assertFalse(schema.existsType(className));
  }
}
