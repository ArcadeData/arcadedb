package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreatePropertyListOfTest extends TestHelper {

  @Test
  public void testListOfInteger() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.nums LIST OF INTEGER");

    // Verify the property was created
    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("nums");
    Assertions.assertNotNull(property);
  }

  @Test
  public void testListOfString() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.tags LIST OF STRING");

    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("tags");
    Assertions.assertNotNull(property);
  }

  @Test
  public void testListOfIntegerIfNotExists() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.nums IF NOT EXISTS LIST OF INTEGER");

    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("nums");
    Assertions.assertNotNull(property);
  }

  @Test
  public void testEmbeddedOf() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE PROPERTY doc.addresses EMBEDDED OF Address");

    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("addresses");
    Assertions.assertNotNull(property);
  }

  // TODO: testPropertyWithAttributes needs execution layer support for boolean conversion
  // @Test
  // public void testPropertyWithAttributes() {
  //   database.command("sql", "CREATE DOCUMENT TYPE doc");
  //   database.command("sql", "CREATE PROPERTY doc.score INTEGER (MANDATORY true, MIN 0, MAX 100)");
  //
  //   var schema = database.getSchema();
  //   var type = schema.getType("doc");
  //   var property = type.getProperty("score");
  //   Assertions.assertNotNull(property);
  //   Assertions.assertTrue(property.isMandatory());
  // }
}
