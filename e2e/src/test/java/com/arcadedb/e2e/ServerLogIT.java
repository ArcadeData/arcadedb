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
package com.arcadedb.e2e;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs after every other class (see junit-platform.properties) and scans the log of the server they all used: a class
 * or a reflective member missing from the native image often surfaces only as a logged error behind a generic failure,
 * or in a background thread no test observes.
 */
@Order(Integer.MAX_VALUE)
class ServerLogIT extends ArcadeContainerTemplate {

  @Test
  void noMissingClassesOrReflectionRegistrations() {
    final String log = ARCADE.getLogs();
    assertThat(log).contains("ArcadeDB Server started");
    assertThat(log).doesNotContain("MissingReflectionRegistrationError", "MissingResourceRegistrationError",
        "ClassNotFoundException", "NoSuchMethodException", "Can't load log handler");
  }
}
