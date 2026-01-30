package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CreatePropertyListOfTest extends TestHelper {

  @Test
  void listOfInteger() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.nums LIST OF INTEGER");

    // Verify the property was created
    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("nums");
    assertThat(property).isNotNull();
  }

  @Test
  void listOfString() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.tags LIST OF STRING");

    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("tags");
    assertThat(property).isNotNull();
  }

  @Test
  void listOfIntegerIfNotExists() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE PROPERTY doc.nums IF NOT EXISTS LIST OF INTEGER");

    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("nums");
    assertThat(property).isNotNull();
  }

  @Test
  void embeddedOf() {
    database.command("sql", "CREATE DOCUMENT TYPE doc");
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE PROPERTY doc.addresses EMBEDDED OF Address");

    var schema = database.getSchema();
    var type = schema.getType("doc");
    var property = type.getProperty("addresses");
    assertThat(property).isNotNull();
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
