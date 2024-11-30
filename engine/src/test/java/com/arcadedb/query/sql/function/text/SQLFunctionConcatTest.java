package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionConcatTest {
  private SQLFunctionConcat function;

  @BeforeEach
  public void setup() {
    function = new SQLFunctionConcat();
  }

  @Test
  public void testConcatSingleField() {
    function.execute(null, null, null, new Object[] { "Hello" }, null);
    function.execute(null, null, null, new Object[] { "World" }, null);
    Object result = function.getResult();
    assertThat(result).isEqualTo("HelloWorld");
  }

  @Test
  public void testConcatWithDelimiter() {
    function.execute(null, null, null, new Object[] { "Hello", " " }, null);
    function.execute(null, null, null, new Object[] { "World", " " }, null);
    Object result = function.getResult();
    assertThat(result).isEqualTo("Hello World");
  }

  @Test
  public void testConcatEmpty() {
    Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  public void testConcatWithNullValues() {
    function.execute(null, null, null, new Object[] { null, " " }, null);
    function.execute(null, null, null, new Object[] { "World", " " }, null);
    Object result = function.getResult();
    assertThat(result).isEqualTo("null World");
  }

  @Test
  public void testQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionConcat", (db) -> {
      setUpDatabase(db);
      ResultSet result = db.query("sql", "select concat(name, ' ') as concat from Person");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("concat")).isEqualTo("Alan Brian");
    });
  }

  private void setUpDatabase(Database db) {
    db.command("sql", "create document type Person");
    db.command("sql", "insert into Person set name = 'Alan'");
    db.command("sql", "insert into Person set name = 'Brian'");
  }
}
