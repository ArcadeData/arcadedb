package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.query.sql.parser.IndexIdentifier;
import com.arcadedb.query.sql.parser.IndexName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class CountFromIndexStepTest {

  private static final String PROPERTY_NAME  = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String ALIAS          = "size";
  private static       String indexName;

  private IndexIdentifier.Type identifierType;

  public CountFromIndexStepTest() {
    this.identifierType = IndexIdentifier.Type.INDEX;
  }

  public static Iterable<Object[]> types() {
    return Arrays.asList(new Object[][] { { IndexIdentifier.Type.INDEX }, { IndexIdentifier.Type.VALUES }, { IndexIdentifier.Type.VALUESASC },
        { IndexIdentifier.Type.VALUESDESC }, });
  }

  @Test
  public void shouldCountRecordsOfIndex() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      DocumentType clazz = TestHelper.createRandomType(db);
      clazz.createProperty(PROPERTY_NAME, Type.STRING);
      String className = clazz.getName();
      indexName = className + "[" + PROPERTY_NAME + "]";
      clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      for (int i = 0; i < 20; i++) {
        MutableDocument document = db.newDocument(className);
        document.set(PROPERTY_NAME, PROPERTY_VALUE);
        document.save();
      }

      className = TestHelper.createRandomType(db).getName();
      IndexName name = new IndexName(-1);
      name.setValue(indexName);
      IndexIdentifier identifier = new IndexIdentifier(-1);
      identifier.setIndexName(name);
      identifier.setIndexNameString(name.getValue());
      identifier.setType(identifierType);

      BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      CountFromIndexStep step = new CountFromIndexStep(identifier, ALIAS, context, false);

      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(20, (long) result.next().getProperty(ALIAS));
      Assertions.assertFalse(result.hasNext());
    });
  }
}
