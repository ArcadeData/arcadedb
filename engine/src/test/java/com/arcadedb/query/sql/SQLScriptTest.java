package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLScriptTest extends TestHelper {
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE DOCUMENT TYPE foo");
      database.command("sql", "insert into foo (name, bar) values ('a', 1)");
      database.command("sql", "insert into foo (name, bar) values ('b', 2)");
      database.command("sql", "insert into foo (name, bar) values ('c', 3)");
    });
  }

  @Test
  public void testQueryOnDeprecated() {
    StringBuilder script = new StringBuilder();
    script.append("begin;\n");
    script.append("let $a = select from foo;\n");
    script.append("commit;\n");
    script.append("return $a;\n");
    ResultSet qResult = database.command("sqlscript", script.toString());

    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  public void testQuery() {
    StringBuilder script = new StringBuilder();
    script.append("begin;\n");
    script.append("let $a = select from foo;\n");
    script.append("commit;\n");
    script.append("return $a;\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  public void testTx() {
    StringBuilder script = new StringBuilder();
    script.append("begin isolation REPEATABLE_READ;\n");
    script.append("let $a = insert into V set test = 'sql script test';\n");
    script.append("commit retry 10;\n");
    script.append("return $a;\n");
    Document qResult = database.command("SQLScript", script.toString()).next().toElement();

    assertThat(qResult).isNotNull();
  }

  @Test
  public void testReturnExpanded() {
    database.transaction(() -> {
      StringBuilder script = new StringBuilder();
      script.append("let $a = insert into V set test = 'sql script test';\n");
      script.append("return $a.asJSON();\n");
      String qResult = database.command("SQLScript", script.toString()).next().getProperty("value").toString();
      assertThat(qResult).isNotNull();
      // VALIDATE JSON
      new JSONObject(qResult);

      script = new StringBuilder();
      script.append("let $a = select from V limit 2;\n");
      script.append("return $a.asJSON();\n");
      String result = database.command("SQLScript", script.toString()).next().getProperty("value").toString();


      assertThat(result).isNotNull();
      result = result.trim();
      assertThat(result).startsWith("{").endsWith("}");

      // VALIDATE JSON
      new JSONObject(result);
    });

  }

  @Test
  public void testSleep() {
    long begin = System.currentTimeMillis();

    StringBuilder script = new StringBuilder();
    script.append("sleep 500");
    database.command("SQLScript", script.toString());

    assertThat(System.currentTimeMillis() - begin >= 500).isTrue();
  }

  //@Test
  public void testConsoleLog() {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'log'\n");
    script.append("console.log 'This is a test of log for ${a}'");
    database.command("SQLScript", script.toString());
  }

  //@Test
  public void testConsoleOutput() {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'output'\n");
    script.append("console.output 'This is a test of log for ${a}'");
    database.command("SQLScript", script.toString());
  }

  //@Test
  public void testConsoleError() {
    StringBuilder script = new StringBuilder();
    script.append("LET $a = 'error';\n");
    script.append("CONSOLE.ERROR 'This is a test of log for ${a}';");
    database.command("SQLScript", script.toString());
  }

  @Test
  public void testReturnObject() {
    StringBuilder script = new StringBuilder();
    script.append("return [{ a: 'b' }]");
    ResultSet result = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(result)).isNotNull();

    assertThat(result.next().<String>getProperty("a")).isEqualTo("b");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  public void testIncrementAndLet() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestCounter");

      StringBuilder script = new StringBuilder();
      script.append("INSERT INTO TestCounter set weight = 3;\n");
      script.append("LET counter = SELECT count(*) as count FROM TestCounter;\n");
      script.append("UPDATE TestCounter SET weight += $counter[0].count RETURN AfTER @this;\n");
      ResultSet qResult = database.command("SQLScript", script.toString());

      assertThat(qResult.hasNext()).isTrue();
      Result result = qResult.next();
      assertThat(result.<Long>getProperty("weight")).isEqualTo(4L);
    });
  }

  @Test
  public void testIf1() {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;\n");
    script.append("if($a[0].one = 1){\n");
    script.append(" return 'OK';\n");
    script.append("}\n");
    script.append("return 'FAIL';\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testIf2() {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;\n");
    script.append("if    ($a[0].one = 1)   { \n");
    script.append(" return 'OK';\n");
    script.append("     }      \n");
    script.append("return 'FAIL';\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testIf3() {
    StringBuilder script = new StringBuilder();
    script.append("let $a = select 1 as one; if($a[0].one = 1){return 'OK';}return 'FAIL';");
    ResultSet qResult = database.command("SQLScript", script.toString());
    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testNestedIf2() {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;\n");
    script.append("if($a[0].one = 1){\n");
    script.append("    if($a[0].one = 'zz'){\n");
    script.append("      return 'FAIL';\n");
    script.append("    }\n");
    script.append("  return 'OK';\n");
    script.append("}\n");
    script.append("return 'FAIL';\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testNestedIf3() {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;\n");
    script.append("if($a[0].one = 'zz'){\n");
    script.append("    if($a[0].one = 1){\n");
    script.append("      return 'FAIL';\n");
    script.append("    }\n");
    script.append("  return 'FAIL';\n");
    script.append("}\n");
    script.append("return 'OK';\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testIfRealQuery() {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select from foo;\n");
    script.append("if($a is not null and $a.size() = 3){\n");
    script.append("  return $a;\n");
    script.append("}\n");
    script.append("return 'FAIL';\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(CollectionUtils.countEntries(qResult)).isEqualTo(3);
  }

  @Test
  public void testIfMultipleStatements() {
    StringBuilder script = new StringBuilder();

    script.append("let $a = select 1 as one;\n");
    script.append("if($a[0].one = 1){\n");
    script.append("  let $b = select 'OK' as ok;\n");
    script.append("  return $b[0].ok;\n");
    script.append("}\n");
    script.append("return 'FAIL';\n");
    ResultSet qResult = database.command("SQLScript", script.toString());

    assertThat(Optional.ofNullable(qResult)).isNotNull();
    assertThat(qResult.next().<String>getProperty("value")).isEqualTo("OK");
  }

  @Test
  public void testSemicolonInString() {
    // testing parsing problem
    StringBuilder script = new StringBuilder();
    script.append("let $a = select 'foo ; bar' as one\n");
    ResultSet qResult = database.command("SQLScript", script.toString());
  }

  @Test
  public void testQuotedRegex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE QuotedRegex2");
      String batch = "INSERT INTO QuotedRegex2 SET regexp=\"'';\"";

      database.command("SQLScript", batch.toString());

      ResultSet result = database.query("sql", "SELECT FROM QuotedRegex2");
      Document doc = result.next().toElement();

      assertThat(result.hasNext()).isFalse();
      assertThat(doc.get("regexp")).isEqualTo("'';");

    });
  }

  @Test
  public void testParameters1() {
    String className = "testParameters1";
    database.getSchema().createVertexType(className);
    database.getSchema().createEdgeType("E");
    String script = "BEGIN;" + "LET $a = CREATE VERTEX " + className + " SET name = :name;" + "LET $b = CREATE VERTEX " + className
        + " SET name = :_name2;"
        + "LET $edge = CREATE EDGE E from $a to $b;" + "COMMIT;" + "RETURN $edge;";

    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "bozo");
    map.put("_name2", "bozi");

    ResultSet rs = database.command("sqlscript", script, map);
    rs.close();

    rs = database.query("sql", "SELECT FROM " + className + " WHERE name = ?", "bozo");

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();
  }

  @Test
  public void testPositionalParameters() {
    String className = "testPositionalParameters";
    database.getSchema().createVertexType(className);
    database.getSchema().createEdgeType("E");
    String script = "BEGIN;" + "LET $a = CREATE VERTEX " + className + " SET name = ?;" + "LET $b = CREATE VERTEX " + className
        + " SET name = ?;"
        + "LET $edge = CREATE EDGE E from $a to $b;" + "COMMIT;" + "RETURN $edge;";

    ResultSet rs = database.command("SQLScript", script, "bozo", "bozi");
    rs.close();

    rs = database.query("sql", "SELECT FROM " + className + " WHERE name = ?", "bozo");

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    rs.close();
  }

  @Test
  public void testInsertJsonNewLines() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("doc");

      final ResultSet result = database.command("sqlscript", "INSERT INTO doc CONTENT {\n" + //
          "\"head\" : {\n" + //
          "  \"vars\" : [ \"item\", \"itemLabel\" ]\n" + //
          "},\n" + //
          "\"results\" : {\n" + //
          "  \"bindings\" : [ {\n" + //
          "    \"item\" : {\n" + //
          "          \"type\" : \"uri\",\n" + //
          "              \"value\" : \"http://www.wikidata.org/entity/Q113997665\"\n" + //
          "        },\n" + //
          "        \"itemLabel\" : {\n" + //
          "          \"xml:lang\" : \"en\",\n" + //
          "              \"type\" : \"literal\",\n" + //
          "              \"value\" : \"ArcadeDB\"\n" + //
          "        }\n" + //
          "      }, {\n" + //
          "        \"item\" : {\n" + //
          "          \"type\" : \"uri\",\n" + //
          "              \"value\" : \"http://www.wikidata.org/entity/Q808716\"\n" + //
          "        },\n" + //
          "        \"itemLabel\" : {\n" + //
          "          \"xml:lang\" : \"en\",\n" + //
          "              \"type\" : \"literal\",\n" + //
          "              \"value\" : \"OrientDB\"\n" + //
          "        }\n" + //
          "      } ]\n" + //
          "    }\n" + //
          "}");

      assertThat(result.hasNext()).isTrue();
      final Result res = result.next();
      assertThat(res.hasProperty("head")).isTrue();
      assertThat(res.hasProperty("results")).isTrue();
    });
  }
}
