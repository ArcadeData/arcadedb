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

import com.arcadedb.engine.PageManager;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for issue #10446.
 * <p>
 * {@code -Darcadedb.profile=low-ram} applied every value of the profile except {@code arcadedb.maxPageRAM}: the startup
 * pass reaches {@code PROFILE} first, whose callback sets the page cache to 16 MB, and then reaches {@code MAX_PAGE_RAM},
 * which has no property of its own and so applied its computed default (a quarter of the heap) over the profile's value.
 * The same pass also made the first embedded open log {@code Configuration profile PARTIALLY applied}, because the profile
 * callback called {@code PageManager.configure()} while the page manager already counted the database being opened.
 * <p>
 * The precedence is an explicit property or variable, then the profile, then the computed default.
 */
class Issue10446ProfileMaxPageRamTest {

  private static final String PROFILE_KEY      = GlobalConfiguration.PROFILE.getKey();
  private static final String MAX_PAGE_RAM_KEY = GlobalConfiguration.MAX_PAGE_RAM.getKey();

  @AfterEach
  void restore() {
    System.clearProperty(PROFILE_KEY);
    System.clearProperty(MAX_PAGE_RAM_KEY);
    GlobalConfiguration.resetAll();
  }

  @Test
  void theProfileKeepsItsPageCacheSizeThroughTheStartupPass() {
    assumeTrue(computedDefaultMB() != 16, "the heap is 64 MB, so the computed default equals the profile's value");

    System.setProperty(PROFILE_KEY, "low-ram");

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(16L);
  }

  @Test
  void anExplicitPageCacheSizeWinsOverTheProfile() {
    assumeTrue(computedDefaultMB() != 16, "the heap is 64 MB, so the computed default equals the profile's value");

    System.setProperty(PROFILE_KEY, "low-ram");
    System.setProperty(MAX_PAGE_RAM_KEY, "24");

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(24L);
  }

  @Test
  void withoutAProfileThePageCacheIsAQuarterOfTheHeap() {
    GlobalConfiguration.MAX_PAGE_RAM.setValue(7);

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(computedDefaultMB());
  }

  /** A pass that reads the same profile twice must not carry the first pass's assignments into the second. */
  @Test
  void aLaterPassWithoutAProfileGoesBackToTheComputedDefault() {
    System.setProperty(PROFILE_KEY, "low-ram");
    GlobalConfiguration.readConfiguration();
    System.clearProperty(PROFILE_KEY);
    GlobalConfiguration.PROFILE.reset();
    GlobalConfiguration.MAX_PAGE_RAM.setValue(7);

    GlobalConfiguration.readConfiguration();

    assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(computedDefaultMB());
  }

  @Test
  void theStartupPassDoesNotWarnThatTheProfileWasPartiallyApplied() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    // THE FIRST EMBEDDED OPEN: THE PAGE MANAGER COUNTS THE DATABASE BEFORE THE CONFIGURATION IS READ
    PageManager.INSTANCE.acquire();
    try {
      System.setProperty(PROFILE_KEY, "low-ram");

      GlobalConfiguration.readConfiguration();

      assertThat(captured.text()).doesNotContain("PARTIALLY applied");
    } finally {
      PageManager.INSTANCE.release();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  /** The warning still belongs to a profile that is changed while a database is open. */
  @Test
  void aProfileChangedAtRuntimeWithADatabaseOpenStillWarns() {
    final CapturingLogger captured = new CapturingLogger();
    final Logger previousLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(captured);
    PageManager.INSTANCE.acquire();
    try {
      GlobalConfiguration.PROFILE.setValue("low-ram");

      assertThat(captured.text()).contains("PARTIALLY applied");
    } finally {
      PageManager.INSTANCE.release();
      LogManager.instance().setLogger(previousLogger);
    }
  }

  private static long computedDefaultMB() {
    return Runtime.getRuntime().maxMemory() / 4 / 1024 / 1024;
  }

  private static final class CapturingLogger implements Logger {
    private final StringBuilder buffer = new StringBuilder();

    String text() {
      return buffer.toString();
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      append(message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      append(message);
    }

    @Override
    public void flush() {
      // NO-OP
    }

    private synchronized void append(final String message) {
      if (message != null)
        buffer.append(message).append('\n');
    }
  }
}
