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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10073: a taken user name is a conflict, not a security refusal. It must be raised as its own exception type, NOT
 * a {@link ServerSecurityException}, because the HTTP boundary maps that whole family to 403 "Security error" - the
 * status a non-root caller gets. Both creation entry points of {@link ServerSecurity} raise it: the node-local
 * {@code createUser} and the cluster-aware {@code createUserClusterWide} (which, with no HA plugin, delegates).
 */
class Issue10073UserAlreadyExistsTest {
  private static final String CONFIG_PATH = "target/test-issue10073-security";

  private ServerSecurity security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
    security = new ServerSecurity(null, new ContextConfiguration(), CONFIG_PATH);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  private JSONObject user(final String name) {
    return new JSONObject().put("name", name).put("password", security.encodePassword("a-long-password"))
        .put("databases", new JSONObject());
  }

  @Test
  void createUserRaisesAConflictNotASecurityFailure() {
    security.createUser(user("alice"));

    assertThatThrownBy(() -> security.createUser(user("alice")))
        .isInstanceOf(UserAlreadyExistsException.class)
        .isNotInstanceOf(ServerSecurityException.class)
        .isNotInstanceOf(SecurityException.class)
        .hasMessageContaining("User 'alice' already exists");
  }

  @Test
  void createUserClusterWideRaisesTheSameConflict() {
    security.createUserClusterWide(user("bob"));

    assertThatThrownBy(() -> security.createUserClusterWide(user("bob")))
        .isInstanceOf(UserAlreadyExistsException.class)
        .isNotInstanceOf(ServerSecurityException.class)
        .hasMessageContaining("User 'bob' already exists");
    assertThat(security.existsUser("bob")).isTrue();
  }
}
