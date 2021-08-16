package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckRecordTypeStepTest {

  @Test
  public void shouldCheckRecordsOfOneType() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = TestHelper.createRandomType(db).getName();

      CommandContext context = new BasicCommandContext();
      CheckRecordTypeStep step = new CheckRecordTypeStep(context, className, false);
      AbstractExecutionStep previous = new AbstractExecutionStep(context, false) {
        boolean done = false;

        @Override
        public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
          InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              ResultInternal item = new ResultInternal();
              item.setElement(db.newDocument(className));
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(10, result.stream().count());
      Assertions.assertFalse(result.hasNext());
    });
  }

  @Test
  public void shouldCheckRecordsOfSubclasses() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      CommandContext context = new BasicCommandContext();
      DocumentType parentClass = TestHelper.createRandomType(db);
      DocumentType childClass = TestHelper.createRandomType(db).addParentType(parentClass);
      CheckRecordTypeStep step = new CheckRecordTypeStep(context, parentClass.getName(), false);
      AbstractExecutionStep previous = new AbstractExecutionStep(context, false) {
        boolean done = false;

        @Override
        public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
          InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              ResultInternal item = new ResultInternal();
              item.setElement(db.newDocument(i % 2 == 0 ? parentClass.getName() : childClass.getName()));
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(10, result.stream().count());
      Assertions.assertFalse(result.hasNext());
    });
  }

  @Test
  public void shouldThrowExceptionWhenTypeIsDifferent() throws Exception {
    try {
      TestHelper.executeInNewDatabase((db) -> {
        CommandContext context = new BasicCommandContext();
        String firstClassName = TestHelper.createRandomType(db).getName();
        String secondClassName = TestHelper.createRandomType(db).getName();
        CheckRecordTypeStep step = new CheckRecordTypeStep(context, firstClassName, false);
        AbstractExecutionStep previous = new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
            InternalResultSet result = new InternalResultSet();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                ResultInternal item = new ResultInternal();
                item.setElement(db.newDocument(i % 2 == 0 ? firstClassName : secondClassName));
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

        step.setPrevious(previous);
        ResultSet result = step.syncPull(context, 20);
        while (result.hasNext()) {
          result.next();
        }
      });
      Assertions.fail("Expected CommandExecutionException");

    } catch (CommandExecutionException e) {
      // OK
    }
  }
}
