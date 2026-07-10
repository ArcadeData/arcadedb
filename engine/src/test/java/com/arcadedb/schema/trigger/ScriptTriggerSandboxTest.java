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
package com.arcadedb.schema.trigger;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GHSA-x9f9-r4m8-9xc2: a JavaScript trigger could reach
 * {@code Java.type("java.lang.Runtime").getRuntime().exec(...)} and obtain OS command execution, because
 * {@code java.lang.*} was in the trigger's host-class-lookup allow-list. After the fix the host-class
 * lookup of any {@code java.lang.*} type (Runtime, ProcessBuilder, System, Class) must be denied so the
 * exec chain cannot even be resolved.
 */
class ScriptTriggerSandboxTest extends TestHelper {

  @Test
  void triggerCannotLookupRuntimeForCommandExecution() {
    database.command("sql", "CREATE DOCUMENT TYPE Target");
    // The trigger tries to obtain java.lang.Runtime through host-class lookup; the fix denies java.lang.*,
    // so building the reference fails and the record insert (which fires the trigger) is aborted with an error.
    database.command("sql",
        """
            CREATE TRIGGER rce_attempt BEFORE CREATE ON TYPE Target \
            EXECUTE JAVASCRIPT 'var r = Java.type("java.lang.Runtime"); r.getRuntime().exec("id"); true;'""");

    assertThatThrownBy(() ->
        database.transaction(() -> database.newDocument("Target").set("x", 1).save()))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void triggerCannotLookupProcessBuilder() {
    database.command("sql", "CREATE DOCUMENT TYPE Target2");
    database.command("sql",
        """
            CREATE TRIGGER pb_attempt BEFORE CREATE ON TYPE Target2 \
            EXECUTE JAVASCRIPT 'var PB = Java.type("java.lang.ProcessBuilder"); new PB("id").start(); true;'""");

    assertThatThrownBy(() ->
        database.transaction(() -> database.newDocument("Target2").set("x", 1).save()))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void benignTriggerStillWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE Ok");
    // A trigger that only uses native JavaScript (Math, arithmetic - no host-class lookup) must keep working
    // after java.lang.* is removed from the allow-list.
    database.command("sql",
        """
            CREATE TRIGGER benign BEFORE CREATE ON TYPE Ok \
            EXECUTE JAVASCRIPT 'var x = Math.max(1, 2) + 3; x > 0;'""");

    database.transaction(() -> database.newDocument("Ok").set("name", "fine").save());

    assertThat(database.query("sql", "SELECT FROM Ok").hasNext()).isTrue();
  }
}
