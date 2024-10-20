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

import java.io.*;

import static com.arcadedb.GlobalConfiguration.TEST;
import static org.assertj.core.api.Assertions.assertThat;

public class ConfigurationTest {
  @Test
  public void testGlobalExport2Json() {
    assertThat(TEST.getValueAsBoolean()).isFalse();
    final String json = GlobalConfiguration.toJSON();
    TEST.setValue(true);
    assertThat(TEST.getValueAsBoolean()).isTrue();
    GlobalConfiguration.fromJSON(json);
    assertThat(TEST.getValueAsBoolean()).isFalse();
  }

  @Test
  public void testContextExport2Json() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(TEST, false);
    assertThat(cfg.getValueAsBoolean(TEST)).isFalse();
    final String json = cfg.toJSON();
    cfg.setValue(TEST, true);
    assertThat(cfg.getValueAsBoolean(TEST)).isTrue();
    cfg.fromJSON(json);
    assertThat(cfg.getValueAsBoolean(TEST)).isFalse();
  }

  @Test
  public void testDump() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    GlobalConfiguration.dumpConfiguration(new PrintStream(out));
    assertThat(out.size() > 0).isTrue();
  }

}
