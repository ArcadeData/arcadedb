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
package com.arcadedb;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8038, found sweeping for the same defect shape the issue reported against the MCP surfaces. The startup
 * dump printed every setting as {@code isHidden() ? "<hidden>" : getValue()}, which is the same predicate the
 * settings readers stopped using: it masks a setting that IS a secret and says nothing about a setting that
 * CONTAINS one. {@code arcadedb.server.defaultDatabases} is the second kind, so an operator who turns on
 * {@code arcadedb.dumpConfigAtStartup} - the documented way to record what a server booted with - wrote every
 * database password into the server log in clear.
 * <p>
 * The dump now renders through {@link GlobalConfiguration#publishableValue(Object)}, the rule
 * {@code GET /api/v1/server} and the MCP {@code get_server_settings} tool already publish under. The
 * {@code <hidden>} spelling a wholly hidden setting prints is unchanged.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8038DumpConfigurationRedactionTest {

  private static final String CONFIGURED = "Universe[albert:einstein:admin];Amiga[Jay:Miner,Jack:Tramiel]";

  @Test
  void theStartupDumpDoesNotPrintTheDefaultDatabasesPasswords() {
    final String dump = dumpWithDefaultDatabases(CONFIGURED);

    assertThat(dump).as("every password embedded in the value").doesNotContain("einstein");
    assertThat(dump).doesNotContain("Miner").doesNotContain("Tramiel");
  }

  /**
   * What the dump is read for survives: which databases the server was told to create and who may reach them
   * with which role. Only the one field the server authenticates with is replaced.
   */
  @Test
  void theStartupDumpStillNamesTheDatabasesAndTheirUsers() {
    final String dump = dumpWithDefaultDatabases(CONFIGURED);

    assertThat(dump).contains(GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey() + " = "
        + GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue(CONFIGURED));
  }

  /** A wholly hidden setting keeps the spelling the dump has always used for it. */
  @Test
  void aWhollyHiddenSettingStillPrintsAsHidden() {
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_ROOT_PASSWORD;
    final Object previous = setting.getValue();
    final boolean wasChanged = setting.isChanged();
    try {
      setting.setValue("s3cr3t-root-password");

      final String dump = dump();

      assertThat(dump).contains(setting.getKey() + " = <hidden>");
      assertThat(dump).doesNotContain("s3cr3t-root-password");
    } finally {
      restore(setting, previous, wasChanged);
    }
  }

  /** A setting carrying no secret is still printed, or the dump cannot say what the server booted with. */
  @Test
  void anOrdinarySettingIsStillPrinted() {
    assertThat(dump()).contains(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey() + " = ");
  }

  private static String dumpWithDefaultDatabases(final String value) {
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_DEFAULT_DATABASES;
    final Object previous = setting.getValue();
    final boolean wasChanged = setting.isChanged();
    try {
      setting.setValue(value);
      return dump();
    } finally {
      restore(setting, previous, wasChanged);
    }
  }

  /**
   * Puts a process-wide setting back exactly as it was, {@link GlobalConfiguration#isChanged()} included, the way
   * {@code Issue7262ConfigurationFileBooleanStrictTest} already does. Neither half alone is that: {@code setValue}
   * marks a setting that was UNSET as explicitly configured, which the whole suite shares and
   * {@code GlobalConfigurationTest} asserts on, while {@code reset()} restores the COMPILED-IN default and would
   * discard a {@code -D} the surrounding run was given.
   */
  private static void restore(final GlobalConfiguration setting, final Object value, final boolean wasChanged) {
    if (wasChanged)
      setting.setValue(value);
    else
      setting.reset();
  }

  private static String dump() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    GlobalConfiguration.dumpConfiguration(new PrintStream(out));
    return out.toString();
  }
}
