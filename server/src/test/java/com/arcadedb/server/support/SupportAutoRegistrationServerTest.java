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
package com.arcadedb.server.support;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A real server holding a key from its settings registers itself as an installation of a mock portal without Studio, and does
 * not when the switch is off or no key is set.
 */
class SupportAutoRegistrationServerTest extends BaseGraphServerTest {
  private static final SupportAutoRegistration.Timing FAST = new SupportAutoRegistration.Timing(20L, new long[] { 20L }, 600_000L);

  private static MockPortal portal;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    try {
      if (portal == null)
        portal = new MockPortal();
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    config.setValue(GlobalConfiguration.SUPPORT_URL, portal.url());
  }

  @AfterEach
  void cleanUp() {
    if (getServer(0) != null) {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "");
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, "");
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_AUTO_REGISTER, true);
    }
    portal.close();
    portal = null;
  }

  private long registrations() {
    return portal.requests.stream().filter(r -> r.path().equals("/api/v1/process-execute")).count();
  }

  private void withKey() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, MockPortal.CLIENT_ID);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, MockPortal.KEY);
  }

  @Test
  void aKeyedServerRegistersItselfOnceAndSendsDiagnosticsWithoutTheKeyInTheBody() {
    withKey();
    getServer(0).getSupportService().startAutoRegistration(FAST);
    await().atMost(Duration.ofSeconds(10)).until(() -> registrations() == 1);
    final MockPortal.Recorded call = portal.requests.stream().filter(r -> r.path().equals("/api/v1/process-execute")).findFirst().get();
    assertThat(call.header("x-api-process")).contains("studio-register-instance");
    assertThat(call.bodyText()).contains("\"diagnostics\"").doesNotContain(MockPortal.KEY);
    assertThat(call.header("authorization")).isEqualTo("Bearer " + MockPortal.KEY);
  }

  @Test
  void doesNotRegisterWhenTheSwitchIsOff() throws Exception {
    withKey();
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_AUTO_REGISTER, false);
    getServer(0).getSupportService().startAutoRegistration(FAST);
    Thread.sleep(400L);
    assertThat(registrations()).isZero();
  }

  @Test
  void doesNotRegisterWithoutAKey() throws Exception {
    getServer(0).getSupportService().startAutoRegistration(FAST);
    Thread.sleep(400L);
    assertThat(portal.requests).isEmpty();
  }

  @Test
  void aRegistrationMadeByStudioIsNotRepeatedAtOnce() throws Exception {
    withKey();
    getServer(0).getSupportService().registerInstallation();
    assertThat(registrations()).isEqualTo(1);
    getServer(0).getSupportService().startAutoRegistration(FAST);
    Thread.sleep(400L);
    assertThat(registrations()).as("the automatic attempt waits out the interval").isEqualTo(1);
  }
}
