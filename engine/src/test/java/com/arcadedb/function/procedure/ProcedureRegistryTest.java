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
package com.arcadedb.function.procedure;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ProcedureRegistryTest {

  @BeforeEach
  void setUp() {
    ProcedureRegistry.clear();
  }

  @AfterEach
  void tearDown() {
    ProcedureRegistry.clear();
  }

  @Test
  void registerProcedure() {
    final Procedure proc = new TestProcedure("test.procedure");

    assertThat(ProcedureRegistry.register(proc)).isTrue();
    assertThat(ProcedureRegistry.hasProcedure("test.procedure")).isTrue();
  }

  @Test
  void registerDuplicateReturnsFalse() {
    final Procedure proc1 = new TestProcedure("test.procedure");
    final Procedure proc2 = new TestProcedure("test.procedure");

    assertThat(ProcedureRegistry.register(proc1)).isTrue();
    assertThat(ProcedureRegistry.register(proc2)).isFalse();
  }

  @Test
  void registerIsCaseInsensitive() {
    final Procedure proc = new TestProcedure("Test.Procedure");

    ProcedureRegistry.register(proc);

    assertThat(ProcedureRegistry.hasProcedure("test.procedure")).isTrue();
    assertThat(ProcedureRegistry.hasProcedure("TEST.PROCEDURE")).isTrue();
    assertThat(ProcedureRegistry.hasProcedure("Test.Procedure")).isTrue();
  }

  @Test
  void registerOrReplaceReplacesExisting() {
    final Procedure proc1 = new TestProcedure("test.procedure");
    final Procedure proc2 = new TestProcedure("test.procedure");

    ProcedureRegistry.register(proc1);
    ProcedureRegistry.registerOrReplace(proc2);

    assertThat(ProcedureRegistry.get("test.procedure")).isSameAs(proc2);
  }

  @Test
  void getProcedure() {
    final Procedure proc = new TestProcedure("my.procedure");
    ProcedureRegistry.register(proc);

    assertThat(ProcedureRegistry.get("my.procedure")).isSameAs(proc);
    assertThat(ProcedureRegistry.get("MY.PROCEDURE")).isSameAs(proc);
  }

  @Test
  void getNonExistentReturnsNull() {
    assertThat(ProcedureRegistry.get("nonexistent")).isNull();
  }

  @Test
  void normalizeApocName() {
    assertThat(ProcedureRegistry.normalizeApocName("apoc.merge.relationship")).isEqualTo("merge.relationship");
    assertThat(ProcedureRegistry.normalizeApocName("APOC.MERGE.RELATIONSHIP")).isEqualTo("merge.relationship");
    assertThat(ProcedureRegistry.normalizeApocName("merge.relationship")).isEqualTo("merge.relationship");
    assertThat(ProcedureRegistry.normalizeApocName("myProc")).isEqualTo("myproc");
  }

  @Test
  void getWithApocPrefix() {
    final Procedure proc = new TestProcedure("merge.relationship");
    ProcedureRegistry.register(proc);

    assertThat(ProcedureRegistry.get("apoc.merge.relationship")).isSameAs(proc);
    assertThat(ProcedureRegistry.hasProcedure("apoc.merge.relationship")).isTrue();
  }

  @Test
  void getProcedureNames() {
    ProcedureRegistry.register(new TestProcedure("proc1"));
    ProcedureRegistry.register(new TestProcedure("proc2"));

    final Set<String> names = ProcedureRegistry.getProcedureNames();

    assertThat(names).containsExactlyInAnyOrder("proc1", "proc2");
  }

  @Test
  void getAllProcedures() {
    final Procedure proc1 = new TestProcedure("proc1");
    final Procedure proc2 = new TestProcedure("proc2");

    ProcedureRegistry.register(proc1);
    ProcedureRegistry.register(proc2);

    final Collection<Procedure> procedures = ProcedureRegistry.getAllProcedures();

    assertThat(procedures).containsExactlyInAnyOrder(proc1, proc2);
  }

  @Test
  void size() {
    assertThat(ProcedureRegistry.size()).isEqualTo(0);

    ProcedureRegistry.register(new TestProcedure("proc1"));
    assertThat(ProcedureRegistry.size()).isEqualTo(1);

    ProcedureRegistry.register(new TestProcedure("proc2"));
    assertThat(ProcedureRegistry.size()).isEqualTo(2);
  }

  @Test
  void unregister() {
    final Procedure proc = new TestProcedure("toRemove");
    ProcedureRegistry.register(proc);

    assertThat(ProcedureRegistry.hasProcedure("toRemove")).isTrue();

    final Procedure removed = ProcedureRegistry.unregister("toRemove");
    assertThat(removed).isSameAs(proc);
    assertThat(ProcedureRegistry.hasProcedure("toRemove")).isFalse();
  }

  @Test
  void unregisterWithApocPrefix() {
    final Procedure proc = new TestProcedure("merge.node");
    ProcedureRegistry.register(proc);

    final Procedure removed = ProcedureRegistry.unregister("apoc.merge.node");
    assertThat(removed).isSameAs(proc);
  }

  @Test
  void unregisterNonExistentReturnsNull() {
    assertThat(ProcedureRegistry.unregister("nonexistent")).isNull();
  }

  @Test
  void clear() {
    ProcedureRegistry.register(new TestProcedure("proc1"));
    ProcedureRegistry.register(new TestProcedure("proc2"));

    assertThat(ProcedureRegistry.size()).isEqualTo(2);

    ProcedureRegistry.clear();

    assertThat(ProcedureRegistry.size()).isEqualTo(0);
  }

  private static class TestProcedure implements Procedure {
    private final String name;

    TestProcedure(final String name) {
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
      return "Test procedure";
    }

    @Override
    public List<String> getYieldFields() {
      return Collections.singletonList("result");
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      return Stream.empty();
    }
  }
}
