package com.arcadedb.query.sql.functions;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionConvertTest {

  @Test
  public void testSQLConversions() throws Exception {
    TestHelper.executeInNewDatabase("testSQLConvert", (db) -> {
      db.transaction((tx) -> {
        db.command("sql", "create document type TestConversion");

        db.command("sql", "insert into TestConversion set string = 'Jay', date = sysdate(), number = 33");

        Document doc = (Document) db.query("sql", "select from TestConversion limit 1").next().toElement();

        db.command("sql", "update TestConversion set selfrid = 'foo" + doc.getIdentity() + "'");

        ResultSet results = db.query("sql", "select string.asString() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof String);

        results = db.query("sql", "select number.asDate() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Date);

        results = db.query("sql", "select number.asDateTime() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Date);

        results = db.query("sql", "select number.asInteger() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Integer);

        results = db.query("sql", "select number.asLong() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Long);

        results = db.query("sql", "select number.asFloat() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Float);

        results = db.query("sql", "select number.asDecimal() as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof BigDecimal);

        results = db.query("sql", "select number.convert('LONG') as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Long);

        results = db.query("sql", "select number.convert('SHORT') as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Short);

        results = db.query("sql", "select number.convert('DOUBLE') as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertTrue(results.next().getProperty("convert") instanceof Double);

        results = db.query("sql", "select selfrid.substring(3).convert('LINK').string as convert from TestConversion");
        Assertions.assertNotNull(results);

        Assertions.assertEquals(results.next().getProperty("convert"), "Jay");
      });
    });
  }
}
