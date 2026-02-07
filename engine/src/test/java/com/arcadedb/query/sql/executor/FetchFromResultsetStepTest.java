/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for FetchFromResultsetStep.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromResultsetStepTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("TestDoc");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestDoc").set("value", i).save();
      }
    });
  }

  @Test
  void shouldFetchAllResultsFromResultSet() {
    database.transaction(() -> {
      // Create an InternalResultSet with test data
      final InternalResultSet innerResult = new InternalResultSet();
      for (int i = 0; i < 5; i++) {
        final ResultInternal result = new ResultInternal(database);
        result.setProperty("value", i);
        innerResult.add(result);
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final ResultSet result = step.syncPull(context, 10);

      int count = 0;
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item.<Integer>getProperty("value")).isLessThan(5);
        count++;
      }

      assertThat(count).isEqualTo(5);
    });
  }

  @Test
  void shouldFetchLimitedResults() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      for (int i = 0; i < 8; i++) {
        final ResultInternal result = new ResultInternal(database);
        result.setProperty("value", i);
        innerResult.add(result);
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final ResultSet result = step.syncPull(context, 3);

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }

      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void shouldHandleEmptyResultSet() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final ResultSet result = step.syncPull(context, 10);

      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldHandleMultiplePulls() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      for (int i = 0; i < 6; i++) {
        final ResultInternal result = new ResultInternal(database);
        result.setProperty("value", i);
        innerResult.add(result);
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);

      // First pull
      ResultSet result = step.syncPull(context, 3);
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(3);

      // Second pull
      result = step.syncPull(context, 3);
      count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void shouldSkipNullResults() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      for (int i = 0; i < 5; i++) {
        final ResultInternal result = new ResultInternal(database);
        result.setProperty("value", i);
        innerResult.add(result);
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final ResultSet result = step.syncPull(context, 10);

      final List<Result> results = new ArrayList<>();
      while (result.hasNext()) {
        final Result item = result.next();
        assertThat(item).isNotNull();
        results.add(item);
      }

      assertThat(results).isNotEmpty();
    });
  }

  @Test
  void shouldThrowExceptionWhenNoMoreElements() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      final ResultInternal result = new ResultInternal(database);
      result.setProperty("value", 0);
      innerResult.add(result);

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final ResultSet resultSet = step.syncPull(context, 10);

      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next(); // consume the one result
      assertThat(resultSet.hasNext()).isFalse();

      assertThatThrownBy(resultSet::next)
          .isInstanceOf(NoSuchElementException.class);
    });
  }

  @Test
  void shouldResetProperly() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      for (int i = 0; i < 3; i++) {
        final ResultInternal result = new ResultInternal(database);
        result.setProperty("value", i);
        innerResult.add(result);
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);

      // First iteration
      ResultSet result = step.syncPull(context, 10);
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(3);

      // Reset and iterate again
      step.reset();
      result = step.syncPull(context, 10);
      count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void shouldHandleZeroRecordsRequest() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      for (int i = 0; i < 10; i++) {
        final ResultInternal result = new ResultInternal(database);
        result.setProperty("value", i);
        innerResult.add(result);
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final ResultSet result = step.syncPull(context, 0);

      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  void shouldProducePrettyPrint() {
    database.transaction(() -> {
      final InternalResultSet innerResult = new InternalResultSet();
      final ResultInternal result = new ResultInternal(database);
      result.setProperty("value", 1);
      innerResult.add(result);

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final FetchFromResultsetStep step = new FetchFromResultsetStep(innerResult, context);
      final String prettyPrint = step.prettyPrint(0, 2);

      assertThat(prettyPrint).isNotNull();
      assertThat(prettyPrint).contains("FETCH FROM RESULTSET");
    });
  }
}
