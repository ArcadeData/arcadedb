package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckClassTypeStepTest {

  @Test
  public void shouldCheckSubclasses() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      DocumentType parentClass = TestHelper.createRandomType(db);
      DocumentType childClass = TestHelper.createRandomType(db).addParentType(parentClass);
      CheckClassTypeStep step = new CheckClassTypeStep(childClass.getName(), parentClass.getName(), context, false);

      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(0, result.stream().count());
    });
  }

  @Test
  public void shouldCheckOneType() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      String className = TestHelper.createRandomType(db).getName();
      CheckClassTypeStep step = new CheckClassTypeStep(className, className, context, false);

      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(0, result.stream().count());
    });
  }

  @Test
  public void shouldThrowExceptionWhenClassIsNotParent() throws Exception {
    try {
      TestHelper.executeInNewDatabase((db) -> {
        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(db);
        CheckClassTypeStep step = new CheckClassTypeStep(TestHelper.createRandomType(db).getName(), TestHelper.createRandomType(db).getName(), context, false);

        step.syncPull(context, 20);
      });
      Assertions.fail("Expected CommandExecutionException");
    } catch (CommandExecutionException e) {
      // OK
    }
  }
}
