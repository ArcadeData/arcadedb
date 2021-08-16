package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.parser.Identifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CountFromClassStepTest {

  private static final String ALIAS = "size";

  @Test
  public void shouldCountRecordsOfClass() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = TestHelper.createRandomType(db).getName();
      for (int i = 0; i < 20; i++) {
        MutableDocument document = db.newDocument(className);
        document.save();
      }

      Identifier classIdentifier = new Identifier(className);

      BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      CountFromClassStep step = new CountFromClassStep(classIdentifier, ALIAS, context, false);

      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(20, (long) result.next().getProperty(ALIAS));
      Assertions.assertFalse(result.hasNext());
    });
  }
}
