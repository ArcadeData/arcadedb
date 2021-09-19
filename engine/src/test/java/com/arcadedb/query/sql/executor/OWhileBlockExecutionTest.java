package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OWhileBlockExecutionTest extends TestHelper {
  public OWhileBlockExecutionTest() {
    autoStartTx = true;
  }

  @Test
  public void testPlain() {

    String className = "testPlain";

    database.getSchema().createDocumentType(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  LET $i = $i + 1;";
    script += "}";
    script += "SELECT FROM " + className + ";";

    ResultSet results = database.execute("sql", script);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      Result item = results.next();
      sum += (Integer) item.getProperty("value");
      tot++;
    }
    Assertions.assertEquals(3, tot);
    Assertions.assertEquals(3, sum);
    results.close();
  }

  @Test
  public void testReturn() {
    String className = "testReturn";

    database.getSchema().createDocumentType(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  IF ($i = 1) {";
    script += "    RETURN;";
    script += "  }";
    script += "  LET $i = $i + 1;";
    script += "}";

    ResultSet results = database.execute("sql", script);
    results.close();
    results = database.query("sql", "SELECT FROM " + className);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      Result item = results.next();
      sum += (Integer) item.getProperty("value");
      tot++;
    }
    Assertions.assertEquals(2, tot);
    Assertions.assertEquals(1, sum);
    results.close();
  }
}
