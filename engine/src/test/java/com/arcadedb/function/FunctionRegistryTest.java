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
package com.arcadedb.function;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FunctionRegistryTest {

  @BeforeEach
  void setUp() {
    FunctionRegistry.clear();
  }

  @AfterEach
  void tearDown() {
    FunctionRegistry.clear();
  }

  @Test
  void registerFunction() {
    final StatelessFunction fn = new TestStatelessFunction("testFunc");

    assertThat(FunctionRegistry.register(fn)).isTrue();
    assertThat(FunctionRegistry.hasFunction("testFunc")).isTrue();
  }

  @Test
  void registerDuplicateReturnsFalse() {
    final StatelessFunction fn1 = new TestStatelessFunction("testFunc");
    final StatelessFunction fn2 = new TestStatelessFunction("testFunc");

    assertThat(FunctionRegistry.register(fn1)).isTrue();
    assertThat(FunctionRegistry.register(fn2)).isFalse();
  }

  @Test
  void registerIsCaseInsensitive() {
    final StatelessFunction fn = new TestStatelessFunction("TestFunc");

    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.hasFunction("testfunc")).isTrue();
    assertThat(FunctionRegistry.hasFunction("TESTFUNC")).isTrue();
    assertThat(FunctionRegistry.hasFunction("TestFunc")).isTrue();
  }

  @Test
  void registerOrReplaceReplacesExisting() {
    final StatelessFunction fn1 = new TestStatelessFunction("testFunc");
    final StatelessFunction fn2 = new TestStatelessFunction("testFunc");

    FunctionRegistry.register(fn1);
    FunctionRegistry.registerOrReplace(fn2);

    assertThat(FunctionRegistry.get("testFunc")).isSameAs(fn2);
  }

  @Test
  void getFunction() {
    final StatelessFunction fn = new TestStatelessFunction("myFunc");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.get("myFunc")).isSameAs(fn);
    assertThat(FunctionRegistry.get("MYFUNC")).isSameAs(fn);
  }

  @Test
  void getNonExistentReturnsNull() {
    assertThat(FunctionRegistry.get("nonexistent")).isNull();
  }

  @Test
  void getStatelessFunction() {
    final StatelessFunction fn = new TestStatelessFunction("statelessFunc");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.getStateless("statelessFunc")).isSameAs(fn);
  }

  @Test
  void getStatelessReturnsNullForRecordFunction() {
    final RecordFunction fn = new TestRecordFunction("recordFunc");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.getStateless("recordFunc")).isNull();
  }

  @Test
  void getRecordFunction() {
    final RecordFunction fn = new TestRecordFunction("recordFunc");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.getRecord("recordFunc")).isSameAs(fn);
  }

  @Test
  void getRecordReturnsNullForStatelessFunction() {
    final StatelessFunction fn = new TestStatelessFunction("statelessFunc");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.getRecord("statelessFunc")).isNull();
  }

  @Test
  void hasStatelessFunction() {
    final StatelessFunction stateless = new TestStatelessFunction("stateless");
    final RecordFunction record = new TestRecordFunction("record");

    FunctionRegistry.register(stateless);
    FunctionRegistry.register(record);

    assertThat(FunctionRegistry.hasStatelessFunction("stateless")).isTrue();
    assertThat(FunctionRegistry.hasStatelessFunction("record")).isFalse();
    assertThat(FunctionRegistry.hasStatelessFunction("nonexistent")).isFalse();
  }

  @Test
  void hasRecordFunction() {
    final StatelessFunction stateless = new TestStatelessFunction("stateless");
    final RecordFunction record = new TestRecordFunction("record");

    FunctionRegistry.register(stateless);
    FunctionRegistry.register(record);

    assertThat(FunctionRegistry.hasRecordFunction("record")).isTrue();
    assertThat(FunctionRegistry.hasRecordFunction("stateless")).isFalse();
    assertThat(FunctionRegistry.hasRecordFunction("nonexistent")).isFalse();
  }

  @Test
  void normalizeApocName() {
    assertThat(FunctionRegistry.normalizeApocName("apoc.text.indexOf")).isEqualTo("text.indexof");
    assertThat(FunctionRegistry.normalizeApocName("APOC.TEXT.INDEXOF")).isEqualTo("text.indexof");
    assertThat(FunctionRegistry.normalizeApocName("text.indexOf")).isEqualTo("text.indexof");
    assertThat(FunctionRegistry.normalizeApocName("myFunc")).isEqualTo("myfunc");
  }

  @Test
  void getWithApocPrefix() {
    final StatelessFunction fn = new TestStatelessFunction("text.indexOf");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.get("apoc.text.indexOf")).isSameAs(fn);
    assertThat(FunctionRegistry.hasFunction("apoc.text.indexOf")).isTrue();
  }

  @Test
  void getFunctionNames() {
    FunctionRegistry.register(new TestStatelessFunction("func1"));
    FunctionRegistry.register(new TestStatelessFunction("func2"));

    final Set<String> names = FunctionRegistry.getFunctionNames();

    assertThat(names).containsExactlyInAnyOrder("func1", "func2");
  }

  @Test
  void getStatelessFunctionNames() {
    FunctionRegistry.register(new TestStatelessFunction("stateless1"));
    FunctionRegistry.register(new TestStatelessFunction("stateless2"));
    FunctionRegistry.register(new TestRecordFunction("record1"));

    final Set<String> names = FunctionRegistry.getStatelessFunctionNames();

    assertThat(names).containsExactlyInAnyOrder("stateless1", "stateless2");
  }

  @Test
  void getRecordFunctionNames() {
    FunctionRegistry.register(new TestStatelessFunction("stateless1"));
    FunctionRegistry.register(new TestRecordFunction("record1"));
    FunctionRegistry.register(new TestRecordFunction("record2"));

    final Set<String> names = FunctionRegistry.getRecordFunctionNames();

    assertThat(names).containsExactlyInAnyOrder("record1", "record2");
  }

  @Test
  void getAllFunctions() {
    final StatelessFunction fn1 = new TestStatelessFunction("func1");
    final RecordFunction fn2 = new TestRecordFunction("func2");

    FunctionRegistry.register(fn1);
    FunctionRegistry.register(fn2);

    final Collection<Function> functions = FunctionRegistry.getAllFunctions();

    assertThat(functions).containsExactlyInAnyOrder(fn1, fn2);
  }

  @Test
  void getAllStatelessFunctions() {
    final StatelessFunction fn1 = new TestStatelessFunction("stateless1");
    final StatelessFunction fn2 = new TestStatelessFunction("stateless2");
    final RecordFunction fn3 = new TestRecordFunction("record1");

    FunctionRegistry.register(fn1);
    FunctionRegistry.register(fn2);
    FunctionRegistry.register(fn3);

    final Collection<StatelessFunction> functions = FunctionRegistry.getAllStatelessFunctions();

    assertThat(functions).containsExactlyInAnyOrder(fn1, fn2);
  }

  @Test
  void getAllRecordFunctions() {
    final StatelessFunction fn1 = new TestStatelessFunction("stateless1");
    final RecordFunction fn2 = new TestRecordFunction("record1");
    final RecordFunction fn3 = new TestRecordFunction("record2");

    FunctionRegistry.register(fn1);
    FunctionRegistry.register(fn2);
    FunctionRegistry.register(fn3);

    final Collection<RecordFunction> functions = FunctionRegistry.getAllRecordFunctions();

    assertThat(functions).containsExactlyInAnyOrder(fn2, fn3);
  }

  @Test
  void size() {
    assertThat(FunctionRegistry.size()).isEqualTo(0);

    FunctionRegistry.register(new TestStatelessFunction("func1"));
    assertThat(FunctionRegistry.size()).isEqualTo(1);

    FunctionRegistry.register(new TestStatelessFunction("func2"));
    assertThat(FunctionRegistry.size()).isEqualTo(2);
  }

  @Test
  void unregister() {
    final StatelessFunction fn = new TestStatelessFunction("toRemove");
    FunctionRegistry.register(fn);

    assertThat(FunctionRegistry.hasFunction("toRemove")).isTrue();

    final Function removed = FunctionRegistry.unregister("toRemove");
    assertThat(removed).isSameAs(fn);
    assertThat(FunctionRegistry.hasFunction("toRemove")).isFalse();
  }

  @Test
  void unregisterWithApocPrefix() {
    final StatelessFunction fn = new TestStatelessFunction("text.join");
    FunctionRegistry.register(fn);

    final Function removed = FunctionRegistry.unregister("apoc.text.join");
    assertThat(removed).isSameAs(fn);
  }

  @Test
  void unregisterNonExistentReturnsNull() {
    assertThat(FunctionRegistry.unregister("nonexistent")).isNull();
  }

  @Test
  void clear() {
    FunctionRegistry.register(new TestStatelessFunction("func1"));
    FunctionRegistry.register(new TestStatelessFunction("func2"));

    assertThat(FunctionRegistry.size()).isEqualTo(2);

    FunctionRegistry.clear();

    assertThat(FunctionRegistry.size()).isEqualTo(0);
  }

  // Test helper implementations

  private static class TestStatelessFunction implements StatelessFunction {
    private final String name;

    TestStatelessFunction(final String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      return null;
    }
  }

  private static class TestRecordFunction implements RecordFunction {
    private final String name;

    TestRecordFunction(final String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return Integer.MAX_VALUE;
    }

    @Override
    public String getDescription() {
      return "";
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      return null;
    }

    @Override
    public RecordFunction config(final Object[] configuredParameters) {
      return this;
    }
  }
}
