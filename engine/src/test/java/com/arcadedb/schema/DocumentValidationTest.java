package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DocumentValidationTest extends TestHelper {

  @Test
  public void testReadOnly() {
    final DocumentType embeddedClazz = database.getSchema().createDocumentType("EmbeddedValidation");
    embeddedClazz.createProperty("int", Type.INTEGER).setReadonly(true);

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setReadonly(true);
    clazz.createProperty("long", Type.LONG).setReadonly(true);
    clazz.createProperty("float", Type.FLOAT).setReadonly(true);
    clazz.createProperty("boolean", Type.BOOLEAN).setReadonly(true);
    clazz.createProperty("binary", Type.BINARY).setReadonly(true);
    clazz.createProperty("byte", Type.BYTE).setReadonly(true);
    clazz.createProperty("date", Type.DATE).setReadonly(true);
    clazz.createProperty("datetime", Type.DATETIME).setReadonly(true);
    clazz.createProperty("decimal", Type.DECIMAL).setReadonly(true);
    clazz.createProperty("double", Type.DOUBLE).setReadonly(true);
    clazz.createProperty("short", Type.SHORT).setReadonly(true);
    clazz.createProperty("string", Type.STRING).setReadonly(true);
    clazz.createProperty("embedded", Type.EMBEDDED).setReadonly(true);
    clazz.createProperty("embeddedList", Type.LIST).setReadonly(true);
    clazz.createProperty("embeddedMap", Type.MAP).setReadonly(true);

    final MutableDocument d = database.newDocument("Validation");
    d.set("int", 10);
    d.set("long", 10);
    d.set("float", 10);
    d.set("boolean", 10);
    d.set("binary", new byte[] {});
    d.set("byte", 10);
    d.set("date", new Date());
    d.set("datetime", new Date());
    d.set("decimal", 10);
    d.set("double", 10);
    d.set("short", 10);
    d.set("string", "yeah");
    d.set("embeddedList", new ArrayList<RID>());
    d.set("embeddedMap", new HashMap<String, RID>());

    final MutableDocument embedded = d.newEmbeddedDocument("EmbeddedValidation", "embedded");
    embedded.set("int", 20);
    embedded.set("long", 20);

    final MutableDocument embeddedInList = d.newEmbeddedDocument("EmbeddedValidation", "embeddedList");
    embeddedInList.set("int", 30);
    embeddedInList.set("long", 30);
    final ArrayList<Document> embeddedList = new ArrayList<Document>();
    embeddedList.add(embeddedInList);

    final MutableDocument embeddedInMap = d.newEmbeddedDocument("EmbeddedValidation", "embeddedMap", "key");
    embeddedInMap.set("int", 30);
    embeddedInMap.set("long", 30);
    final Map<String, Document> embeddedMap = new HashMap<>();
    embeddedMap.put("testEmbedded", embeddedInMap);

    database.transaction(() -> {
      d.save();

      checkReadOnlyField(d, "int");
      checkReadOnlyField(d, "long");
      checkReadOnlyField(d, "float");
      checkReadOnlyField(d, "boolean");
      checkReadOnlyField(d, "binary");
      checkReadOnlyField(d, "byte");
      checkReadOnlyField(d, "date");
      checkReadOnlyField(d, "datetime");
      checkReadOnlyField(d, "decimal");
      checkReadOnlyField(d, "double");
      checkReadOnlyField(d, "short");
      checkReadOnlyField(d, "string");
      checkReadOnlyField(d, "embedded");
      checkReadOnlyField(d, "embeddedList");
      checkReadOnlyField(d, "embeddedMap");
    });
  }

  @Test
  public void testRequiredValidationAPI() {
    final DocumentType embeddedClazz = database.getSchema().createDocumentType("EmbeddedValidation");
    embeddedClazz.createProperty("int", Type.INTEGER).setMandatory(true);

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setMandatory(true);
    clazz.createProperty("long", Type.LONG).setMandatory(true);
    clazz.createProperty("float", Type.FLOAT).setMandatory(true);
    clazz.createProperty("boolean", Type.BOOLEAN).setMandatory(true);
    clazz.createProperty("binary", Type.BINARY).setMandatory(true);
    clazz.createProperty("byte", Type.BYTE).setMandatory(true);
    clazz.createProperty("date", Type.DATE).setMandatory(true);
    clazz.createProperty("datetime", Type.DATETIME).setMandatory(true);
    clazz.createProperty("decimal", Type.DECIMAL).setMandatory(true);
    clazz.createProperty("double", Type.DOUBLE).setMandatory(true);
    clazz.createProperty("short", Type.SHORT).setMandatory(true);
    clazz.createProperty("string", Type.STRING).setMandatory(true);
    clazz.createProperty("embedded", Type.EMBEDDED).setMandatory(true);

    clazz.createProperty("embeddedList", Type.LIST).setMandatory(true);
    clazz.createProperty("embeddedMap", Type.MAP).setMandatory(true);

    final MutableDocument d = database.newDocument("Validation");
    d.set("int", 10);
    d.set("long", 10);
    d.set("float", 10);
    d.set("boolean", 10);
    d.set("binary", new byte[] {});
    d.set("byte", 10);
    d.set("date", new Date());
    d.set("datetime", new Date());
    d.set("decimal", 10);
    d.set("double", 10);
    d.set("short", 10);
    d.set("string", "yeah");

    d.set("embeddedList", new ArrayList<RID>());
    d.set("embeddedMap", new HashMap<String, RID>());

    final MutableDocument embedded = d.newEmbeddedDocument("EmbeddedValidation", "embedded");
    embedded.set("int", 20);
    embedded.set("long", 20);

    final MutableDocument embeddedInList = d.newEmbeddedDocument("EmbeddedValidation", "embeddedList");
    embeddedInList.set("int", 30);
    embeddedInList.set("long", 30);
    final ArrayList<Document> embeddedList = new ArrayList<Document>();
    embeddedList.add(embeddedInList);

    final MutableDocument embeddedInMap = d.newEmbeddedDocument("EmbeddedValidation", "embeddedMap", "key");
    embeddedInMap.set("int", 30);
    embeddedInMap.set("long", 30);
    final Map<String, Document> embeddedMap = new HashMap<>();
    embeddedMap.put("testEmbedded", embeddedInMap);

    d.validate();

    checkRequireField(d, "int");
    checkRequireField(d, "long");
    checkRequireField(d, "float");
    checkRequireField(d, "boolean");
    checkRequireField(d, "binary");
    checkRequireField(d, "byte");
    checkRequireField(d, "date");
    checkRequireField(d, "datetime");
    checkRequireField(d, "decimal");
    checkRequireField(d, "double");
    checkRequireField(d, "short");
    checkRequireField(d, "string");
    checkRequireField(d, "embedded");
    checkRequireField(d, "embeddedList");
    checkRequireField(d, "embeddedMap");
  }

  @Test
  public void testDefaultValueIsSetWithSQL() {
    final DocumentType clazz = database.getSchema().createDocumentType("Validation");

    database.command("sql", "create property Validation.long LONG (default 1)");
    database.command("sql", "create property Validation.string STRING (default \"1\")");
    database.command("sql", "create property Validation.dat DATETIME (default sysdate('YYYY-MM-DD HH:MM:SS'))");

    assertThat(clazz.getProperty("long").getDefaultValue()).isEqualTo(1L);
    assertThat(clazz.getProperty("string").getDefaultValue()).isEqualTo("1");
    assertThat(clazz.getProperty("dat").getDefaultValue() instanceof Date).isTrue();

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Validation");
      d.save();
      assertThat(d.get("long")).isEqualTo(1L);
      assertThat(d.get("string")).isEqualTo("1");
      assertThat(d.get("dat") instanceof Date);
    });
  }

  @Test
  public void testDefaultNotNullValueIsSetWithSQL() {
    final DocumentType clazz = database.getSchema().createDocumentType("Validation");

    database.command("sql", "create property Validation.string STRING (notnull, default \"1\")");

    assertThat(clazz.getProperty("string").getDefaultValue()).isEqualTo("1");

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Validation");
      d.save();
      assertThat(d.get("string")).isEqualTo("1");
    });
  }

  @Test
  public void testDefaultNotNullMandatoryValueIsSetWithSQL() {
    final DocumentType clazz = database.getSchema().createDocumentType("Validation");

    database.command("sql", "create property Validation.string STRING (mandatory true, notnull true, default \"Hi\")");
    database.command("sql", "create property Validation.dat DATETIME (mandatory true, default null)");

    assertThat(clazz.getProperty("string").getDefaultValue()).isEqualTo("Hi");
    assertThat(clazz.getProperty("dat").getDefaultValue()).isNull();

    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Validation");
      d.save();
      assertThat(d.get("string")).isEqualTo("Hi");
      assertThat(d.get("dat")).isNull();

      final ResultSet resultSet = database.command("sql", "insert into Validation set string = null");

      assertThat(resultSet.hasNext()).isTrue();
      final Result result = resultSet.next();

      assertThat(result.<String>getProperty("string")).isEqualTo("Hi");
      assertThat(result.hasProperty("dat")).isTrue();
      assertThat(result.<String>getProperty("dat")).isNull();
    });
  }

  @Test
  public void testRequiredValidationSQL() {
    final DocumentType clazz = database.getSchema().createDocumentType("Validation");

    database.command("sql", "create property Validation.int INTEGER (mandatory true)");

    assertThat(clazz.getProperty("int").isMandatory()).isTrue();

    final MutableDocument d = database.newDocument("Validation");
    d.set("int", 10);

    d.validate();

    checkRequireField(d, "int");
  }

  @Test
  public void testRequiredValidationEdge() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.command("sql", "create property E.id STRING (mandatory true)");

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V").save();
      final MutableVertex v2 = database.newVertex("V").save();
      try {
        v1.newEdge("E", v2, true);
        Assertions.fail();
      } catch (ValidationException e) {
        // EXPECTED
      }

      final MutableEdge e = v1.newEdge("E", v2, true, "id", "12345");
      assertThat(e.getString("id")).isEqualTo("12345");
    });
  }

  @Test
  public void testRequiredValidationEdgeSQL() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.command("sql", "create property E.a STRING (mandatory true)");
    database.command("sql", "create property E.b STRING (mandatory true)");
    database.command("sql", "create property E.c STRING (mandatory true)");

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V").save();
      final MutableVertex v2 = database.newVertex("V").save();
      try {
        database.command("sql", "create edge E from ? to ?", v1, v2);
        Assertions.fail();
      } catch (ValidationException e) {
        // EXPECTED
      }

      try {
        database.command("sql", "create edge E from ? to ? set a = '12345'", v1, v2);
        Assertions.fail();
      } catch (ValidationException e) {
        // EXPECTED
      }

      try {
        database.command("sql", "create edge E from ? to ? set a = '12345', b = '4444'", v1, v2);
        Assertions.fail();
      } catch (ValidationException e) {
        // EXPECTED
      }

      final Edge e = database.command("sql", "create edge E from ? to ? set a = '12345', b = '4444', c = '2222'", v1, v2)
          .nextIfAvailable().getEdge().get();
      assertThat(e.getString("a")).isEqualTo("12345");
      assertThat(e.getString("b")).isEqualTo("4444");
      assertThat(e.getString("c")).isEqualTo("2222");
    });
  }

  @Test
  public void testValidationNotValidEmbedded() {
    final DocumentType embeddedClazz = database.getSchema().createDocumentType("EmbeddedValidation");
    embeddedClazz.createProperty("int", Type.INTEGER).setMandatory(true);

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setMandatory(true);
    clazz.createProperty("long", Type.LONG).setMandatory(true);
    clazz.createProperty("embedded", Type.EMBEDDED).setMandatory(true);

    final MutableDocument d = database.newDocument("Validation");
    d.set("int", 30);
    d.set("long", 30);

    final MutableDocument embedded = d.newEmbeddedDocument("EmbeddedValidation", "embedded");
    embedded.set("test", "test");
    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (final ValidationException e) {
      assertThat(e.toString().contains("int")).isTrue();
    }
  }

  @Test
  public void testValidationNotValidEmbeddedList() {
    final DocumentType embeddedClazz = database.getSchema().createDocumentType("EmbeddedValidation");
    embeddedClazz.createProperty("int", Type.INTEGER).setMandatory(true);
    embeddedClazz.createProperty("long", Type.LONG).setMandatory(true);

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setMandatory(true);
    clazz.createProperty("long", Type.LONG).setMandatory(true);
    clazz.createProperty("embeddedList", Type.LIST).setMandatory(true);

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("int", 30);
    d.set("long", 30);

    final ArrayList<Document> embeddedList = new ArrayList<>();
    d.set("embeddedList", embeddedList);

    final MutableDocument embeddedInList = d.newEmbeddedDocument("EmbeddedValidation", "embeddedList");
    embeddedInList.set("int", 30);
    embeddedInList.set("long", 30);

    final MutableDocument embeddedInList2 = d.newEmbeddedDocument("EmbeddedValidation", "embeddedList");
    embeddedInList2.set("int", 30);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (final ValidationException e) {
      assertThat(e.toString().contains("long")).isTrue();
    }
  }

  @Test
  public void testValidationNotValidEmbeddedMap() {
    final DocumentType embeddedClazz = database.getSchema().createDocumentType("EmbeddedValidation");
    embeddedClazz.createProperty("int", Type.INTEGER).setMandatory(true);
    embeddedClazz.createProperty("long", Type.LONG).setMandatory(true);

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setMandatory(true);
    clazz.createProperty("long", Type.LONG).setMandatory(true);
    clazz.createProperty("embeddedMap", Type.MAP).setMandatory(true);

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("int", 30);
    d.set("long", 30);
    final Map<String, Document> embeddedMap = new HashMap<String, Document>();
    d.set("embeddedMap", embeddedMap);

    final MutableDocument embeddedInMap = d.newEmbeddedDocument("EmbeddedValidation", "embeddedList");
    embeddedInMap.set("int", 30);
    embeddedInMap.set("long", 30);
    embeddedMap.put("1", embeddedInMap);

    final MutableDocument embeddedInMap2 = d.newEmbeddedDocument("EmbeddedValidation", "embeddedList");
    embeddedInMap2.set("int", 30);
    embeddedMap.put("2", embeddedInMap2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (final ValidationException e) {
      assertThat(e.toString().contains("long")).isTrue();
    }
  }

  @Test
  public void testMaxValidation() {
    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setMax("11");
    clazz.createProperty("long", Type.LONG).setMax("11");
    clazz.createProperty("float", Type.FLOAT).setMax("11");
    clazz.createProperty("binary", Type.BINARY).setMax("11");
    clazz.createProperty("byte", Type.BYTE).setMax("11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, 1);
    SimpleDateFormat format = new SimpleDateFormat(database.getSchema().getDateFormat());
    clazz.createProperty("date", Type.DATE).setMax(format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = new SimpleDateFormat(database.getSchema().getDateTimeFormat());
    clazz.createProperty("datetime", Type.DATETIME).setMax(format.format(cal.getTime()));

    clazz.createProperty("decimal", Type.DECIMAL).setMax("11");
    clazz.createProperty("double", Type.DOUBLE).setMax("11");
    clazz.createProperty("short", Type.SHORT).setMax("11");
    clazz.createProperty("string", Type.STRING).setMax("11");
    clazz.createProperty("embeddedList", Type.LIST).setMax("2");
    clazz.createProperty("embeddedMap", Type.MAP).setMax("2");

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("int", 11);
    d.set("long", 11);
    d.set("float", 11);
    d.set("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 });
    d.set("byte", 11);
    d.set("date", new Date());
    d.set("datetime", new Date());
    d.set("decimal", 10);
    d.set("double", 10);
    d.set("short", 10);
    d.set("string", "yeah");
    d.set("embeddedList", Arrays.asList("a", "b"));
    final HashMap<String, String> cont = new HashMap<String, String>();
    cont.put("one", "one");
    cont.put("two", "one");
    d.set("embeddedMap", cont);

    d.validate();

    checkFieldValue(d, "int", 12);
    checkFieldValue(d, "long", 12);
    checkFieldValue(d, "float", 20);
    checkFieldValue(d, "binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 });

    checkFieldValue(d, "byte", 20);
    cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, 2);
    checkFieldValue(d, "date", cal.getTime());
    checkFieldValue(d, "datetime", cal.getTime());
    checkFieldValue(d, "decimal", 20);
    checkFieldValue(d, "double", 20);
    checkFieldValue(d, "short", 20);
    checkFieldValue(d, "string", "0123456789101112");
    checkFieldValue(d, "embeddedList", Arrays.asList("a", "b", "d"));
    final HashMap<String, String> con1 = new HashMap<>();
    con1.put("one", "one");
    con1.put("two", "one");
    con1.put("three", "one");

    checkFieldValue(d, "embeddedMap", con1);
  }

  @Test
  public void testMinValidation() {
    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setMin("11");
    clazz.createProperty("long", Type.LONG).setMin("11");
    clazz.createProperty("float", Type.FLOAT).setMin("11");
    // clazz.createProperty("boolean", Type.BOOLEAN) //no meaning
    clazz.createProperty("binary", Type.BINARY).setMin("11");
    clazz.createProperty("byte", Type.BYTE).setMin("11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = new SimpleDateFormat(database.getSchema().getDateFormat());
    clazz.createProperty("date", Type.DATE).setMin(format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = new SimpleDateFormat(database.getSchema().getDateTimeFormat());
    clazz.createProperty("datetime", Type.DATETIME).setMin(format.format(cal.getTime()));

    clazz.createProperty("decimal", Type.DECIMAL).setMin("11");
    clazz.createProperty("double", Type.DOUBLE).setMin("11");
    clazz.createProperty("short", Type.SHORT).setMin("11");
    clazz.createProperty("string", Type.STRING).setMin("11");

    clazz.createProperty("embeddedList", Type.LIST).setMin("1");
    clazz.createProperty("embeddedMap", Type.MAP).setMin("1");

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("int", 11);
    d.set("long", 11);
    d.set("float", 11);
    d.set("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 });
    d.set("byte", 11);

    cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, 1);
    d.set("date", cal.getTime());
    d.set("datetime", cal.getTime());
    d.set("decimal", 12);
    d.set("double", 12);
    d.set("short", 12);
    d.set("string", "yeahyeahyeah");
    d.set("embeddedList", Arrays.asList("a"));
    final Map<String, String> map = new HashMap<>();
    map.put("some", "value");
    d.set("embeddedMap", map);

    d.validate();

    checkFieldValue(d, "int", 10);
    checkFieldValue(d, "long", 10);
    checkFieldValue(d, "float", 10);
    checkFieldValue(d, "binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    checkFieldValue(d, "byte", 10);

    cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -1);
    checkFieldValue(d, "date", cal.getTime());
    checkFieldValue(d, "datetime", new Date());
    checkFieldValue(d, "decimal", 10);
    checkFieldValue(d, "double", 10);
    checkFieldValue(d, "short", 10);
    checkFieldValue(d, "string", "01234");
    checkFieldValue(d, "embeddedList", new ArrayList<>());
    checkFieldValue(d, "embeddedMap", new HashMap<>());
  }

  @Test
  public void testNotNullValidation() {
    database.getSchema().createDocumentType("EmbeddedValidation");

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setNotNull(true);
    clazz.createProperty("long", Type.LONG).setNotNull(true);
    clazz.createProperty("float", Type.FLOAT).setNotNull(true);
    clazz.createProperty("boolean", Type.BOOLEAN).setNotNull(true);
    clazz.createProperty("binary", Type.BINARY).setNotNull(true);
    clazz.createProperty("byte", Type.BYTE).setNotNull(true);
    clazz.createProperty("date", Type.DATE).setNotNull(true);
    clazz.createProperty("datetime", Type.DATETIME).setNotNull(true);
    clazz.createProperty("decimal", Type.DECIMAL).setNotNull(true);
    clazz.createProperty("double", Type.DOUBLE).setNotNull(true);
    clazz.createProperty("short", Type.SHORT).setNotNull(true);
    clazz.createProperty("string", Type.STRING).setNotNull(true);
    clazz.createProperty("embedded", Type.EMBEDDED).setNotNull(true);

    clazz.createProperty("embeddedList", Type.LIST).setNotNull(true);
    clazz.createProperty("embeddedMap", Type.MAP).setNotNull(true);

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("int", 12);
    d.set("long", 12);
    d.set("float", 12);
    d.set("boolean", true);
    d.set("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
    d.set("byte", 12);
    d.set("date", new Date());
    d.set("datetime", new Date());
    d.set("decimal", 12);
    d.set("double", 12);
    d.set("short", 12);
    d.set("string", "yeah");
    d.newEmbeddedDocument("EmbeddedValidation", "embedded").set(" test", " test");
    d.set("embeddedList", new ArrayList<>());
    d.set("embeddedMap", new HashMap<>());

    d.validate();

    checkFieldValue(d, "int", null);
    checkFieldValue(d, "long", null);
    checkFieldValue(d, "float", null);
    checkFieldValue(d, "boolean", null);
    checkFieldValue(d, "binary", null);
    checkFieldValue(d, "byte", null);
    checkFieldValue(d, "date", null);
    checkFieldValue(d, "datetime", null);
    checkFieldValue(d, "decimal", null);
    checkFieldValue(d, "double", null);
    checkFieldValue(d, "short", null);
    checkFieldValue(d, "string", null);
    checkFieldValue(d, "embedded", null);
    checkFieldValue(d, "embeddedList", null);
    checkFieldValue(d, "embeddedMap", null);
  }

  @Test
  public void testNotNullSave() {
    database.getSchema().createDocumentType("EmbeddedValidation");

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("int", Type.INTEGER).setNotNull(true);
    clazz.createProperty("long", Type.LONG).setNotNull(true);
    clazz.createProperty("float", Type.FLOAT).setNotNull(true);
    clazz.createProperty("boolean", Type.BOOLEAN).setNotNull(true);
    clazz.createProperty("binary", Type.BINARY).setNotNull(true);
    clazz.createProperty("byte", Type.BYTE).setNotNull(true);
    clazz.createProperty("date", Type.DATE).setNotNull(true);
    clazz.createProperty("datetime", Type.DATETIME).setNotNull(true);
    clazz.createProperty("decimal", Type.DECIMAL).setNotNull(true);
    clazz.createProperty("double", Type.DOUBLE).setNotNull(true);
    clazz.createProperty("short", Type.SHORT).setNotNull(true);
    clazz.createProperty("string", Type.STRING).setNotNull(true);
    clazz.createProperty("embedded", Type.EMBEDDED).setNotNull(true);

    clazz.createProperty("embeddedList", Type.LIST).setNotNull(true);
    clazz.createProperty("embeddedMap", Type.MAP).setNotNull(true);

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("int", 12);
    d.set("long", 12);
    d.set("float", 12);
    d.set("boolean", true);
    d.set("binary", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
    d.set("byte", 12);
    d.set("date", new Date());
    d.set("datetime", new Date());
    d.set("decimal", 12);
    d.set("double", 12);
    d.set("short", 12);
    d.set("string", "yeah");
    d.newEmbeddedDocument("EmbeddedValidation", "embedded").set(" test", " test");
    d.set("embeddedList", new ArrayList<>());
    d.set("embeddedMap", new HashMap<>());

    database.transaction(() -> {
      d.save();
    });

    checkFieldValue(d, "int", null);
    checkFieldValue(d, "long", null);
    checkFieldValue(d, "float", null);
    checkFieldValue(d, "boolean", null);
    checkFieldValue(d, "binary", null);
    checkFieldValue(d, "byte", null);
    checkFieldValue(d, "date", null);
    checkFieldValue(d, "datetime", null);
    checkFieldValue(d, "decimal", null);
    checkFieldValue(d, "double", null);
    checkFieldValue(d, "short", null);
    checkFieldValue(d, "string", null);
    checkFieldValue(d, "embedded", null);
    checkFieldValue(d, "embeddedList", null);
    checkFieldValue(d, "embeddedMap", null);
  }

  @Test
  public void testRegExpValidation() {
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType("Validation");
    clazz.getOrCreateProperty("string", Type.STRING).setRegexp("[^Z]*");

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("string", "yeah");
    d.validate();

    checkFieldValue(d, "string", "yaZah");
  }

  @Test
  public void testRegExpValidationFromSQL() {
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType("Validation");

    database.command("sql", "create property Validation.anychars string (regexp '.*')");

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("anychars", "yeah");
    d.validate();

    // CHECK ALTER PROPERTY
    database.command("sql", "alter property Validation.anychars regexp '[^Z]*'");
    d.set("anychars", "yeah");
    d.validate();

    checkFieldValue(d, "anychars", "yaZah");
  }

  @Test
  public void testPropertyMetadataAreSavedAndReloadded() {
    database.getSchema().createDocumentType("EmbeddedValidation");

    final DocumentType clazz = database.getSchema().createDocumentType("Validation");
    clazz.createProperty("string", Type.STRING).setNotNull(true).setReadonly(true).setMandatory(true);

    final MutableDocument d = database.newDocument(clazz.getName());
    d.set("string", "yeah");

    database.transaction(() -> {
      d.save();
    });

    checkFieldValue(d, "string", null);
    checkRequireField(d, "string");
    checkReadOnlyField(d, "string");

    database.close();
    database = factory.open();

    final DocumentType clazzLoaded = database.getSchema().getType("Validation");
    final Property property = clazzLoaded.getPropertyIfExists("string");

    assertThat(property.isMandatory()).isTrue();
    assertThat(property.isReadonly()).isTrue();
    assertThat(property.isNotNull()).isTrue();
  }

  @Test
  public void testMinMaxNotApplicable() {
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType("Validation");
    try {
      clazz.createProperty("invString", Type.STRING).setMin("-1");
      fail("");
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }

    try {
      clazz.createProperty("invBinary", Type.LIST).setMax("-1");
      fail("");
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  public void testEmbeddedDocumentConversion() {
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType("Validation");
    MutableDocument v = clazz.newRecord();
    v.set("embedded", Map.of("value", 300, "@type", "Validation"));
    final EmbeddedDocument embedded = v.getEmbedded("embedded");
    assertThat(embedded).isNotNull();
    assertThat(embedded.getInteger("value")).isEqualTo(300);
  }

  private void checkFieldValue(final Document toCheck, final String field, final Object newValue) {
    try {
      final MutableDocument newD = database.newDocument(toCheck.getTypeName()).fromMap(toCheck.toMap());
      newD.set(field, newValue);
      newD.validate();
      fail("");
    } catch (final ValidationException v) {
    }
  }

  private void checkRequireField(final MutableDocument toCheck, final String fieldName) {
    try {
      final MutableDocument newD = database.newDocument(toCheck.getTypeName()).fromMap(toCheck.toMap());
      newD.remove(fieldName);
      newD.validate();
      fail("");
    } catch (final ValidationException v) {
    }
  }

  private void checkReadOnlyField(final MutableDocument toCheck, final String fieldName) {
    try {
      toCheck.remove(fieldName);
      toCheck.validate();
      fail("");
    } catch (final ValidationException v) {
    }
  }
}
