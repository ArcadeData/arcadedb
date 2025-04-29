package com.arcadedb.query.sql.function.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for SQLFunctionIfEmpty
 */
public class SQLFunctionIfEmptyTest extends TestHelper {

  @Test
  public void testFunctionInstance() {
    SQLFunctionIfEmpty function = new SQLFunctionIfEmpty();

    assertThat(function.getName()).isEqualTo("ifempty");
    assertThat(function.getSyntax()).isEqualTo("Syntax error: ifempty(<field|value>, <return_value_if_empty> [,<return_value_if_not_empty>])");
  }

  @Test
  public void testDirectExecution() {
    SQLFunctionIfEmpty function = new SQLFunctionIfEmpty();

    // Case 1: Non-empty string
    Object result1 = function.execute(null, null, null, new Object[] { "nonEmptyString", "defaultValue" }, null);
    assertThat(result1).isEqualTo("nonEmptyString");

    // Case 2: Empty string
    Object result2 = function.execute(null, null, null, new Object[] { "", "defaultValue" }, null);
    assertThat(result2).isEqualTo("defaultValue");

    // Case 3: Non-empty collection
    List<String> nonEmptyList = Arrays.asList("item1", "item2");
    Object result3 = function.execute(null, null, null, new Object[] { nonEmptyList, "defaultValue" }, null);
    assertThat(result3).isEqualTo(nonEmptyList);

    // Case 4: Empty collection
    List<String> emptyList = Collections.emptyList();
    Object result4 = function.execute(null, null, null, new Object[] { emptyList, "defaultValue" }, null);
    assertThat(result4).isEqualTo("defaultValue");

    // Case 5: Non string/collection value
    Object result5 = function.execute(null, null, null, new Object[] { 42, "defaultValue" }, null);
    assertThat(result5).isEqualTo(42);

    // Case 6: Null value (not considered empty by this function)
    Object result6 = function.execute(null, null, null, new Object[] { null, "defaultValue" }, null);
    assertThat(result6).isNull();

    // Case 7: Empty array
    String[] emptyArray = new String[] {};
    Object result7 = function.execute(null, null, null, new Object[] { emptyArray, "defaultValue" }, null);
    assertThat(result7).isEqualTo("defaultValue");

    // Case 8: Non-empty array
    String[] nonEmptyArray = new String[] { "item1", "item2" };
    Object result8 = function.execute(null, null, null, new Object[] { nonEmptyArray, "defaultValue" }, null);
    assertThat(result8).isEqualTo(nonEmptyArray);
  }

  @Test
  public void testSQLQueryWithIfEmpty() {
    database.transaction(() -> {
      database.command("sql", "create document type TestIfEmpty");
      database.command("sql",
          "insert into TestIfEmpty content {'name': 'Test1', 'description': 'Description 1', 'tags': ['tag1', 'tag2']}");
      database.command("sql", "insert into TestIfEmpty content {'name': 'Test2', 'description': '', 'tags': []}");
      database.command("sql", "insert into TestIfEmpty content {'name': 'Test3', 'description': 'Description 3', 'tags': null}");

      // Test ifempty with string field
      ResultSet result = database.query("sql",
          "select name, ifempty(description, 'No description') AS description  from TestIfEmpty order by name");

      // First record has non-empty description
      Result next = result.next();
      assertThat(next.<String>getProperty("description")).isEqualTo("Description 1");

      // Second record has empty description
      assertThat(result.next().<String>getProperty("description")).isEqualTo("No description");

      // Third record has non-empty description
      assertThat(result.next().<String>getProperty("description")).isEqualTo("Description 3");

      // Test ifempty with array field
      result = database.query("sql",
          "select name, ifempty(tags, ['default']) as tags from TestIfEmpty order by name");

      // First record has non-empty tags
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<List<?>>getProperty("tags")).isInstanceOf(List.class);

      // Second record has empty tags
      assertThat(result.hasNext()).isTrue();
      Object tags = result.next().getProperty("tags");
      assertThat(tags).isInstanceOf(List.class);
      assertThat((List<?>) tags).hasSize(1);
      assertThat(((List<?>) tags).getFirst()).isEqualTo("default");

      // Third record has null tags (not considered empty)
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Object>getProperty("tags")).isNull();

      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  public void testCombinedIfEmptyAndIfNullInSQL() {
    database.transaction(() -> {
      database.command("sql", "create document type TestCombined");
      database.command("sql", "insert into TestCombined content {'name': 'Test1', 'value': 'Value1'}");
      database.command("sql", "insert into TestCombined content {'name': 'Test2', 'value': ''}");
      database.command("sql", "insert into TestCombined content {'name': 'Test3', 'value': null}");

      // Combine ifnull and ifempty to handle both null and empty values
      ResultSet result = database.query("sql",
          "select name, ifempty(ifnull(value, ''), 'No value') as value from TestCombined order by name");

      // First record has non-empty value
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("value")).isEqualTo("Value1");

      // Second record has empty value
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("value")).isEqualTo("No value");

      // Third record has null value
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("value")).isEqualTo("No value");

      assertThat(result.hasNext()).isFalse();
    });
  }
}
