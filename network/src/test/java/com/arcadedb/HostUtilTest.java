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

import com.arcadedb.network.HostUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HostUtilTest {
  @Test
  public void testIPv4() {
    final String[] parts = HostUtil.parseHostAddress("10.33.5.22", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts.length).isEqualTo(2);
    assertThat(parts[0]).isEqualTo("10.33.5.22");
    assertThat(parts[1]).isEqualTo(HostUtil.CLIENT_DEFAULT_PORT);
  }

  @Test
  public void testIPv4WithPort() {
    final String[] parts = HostUtil.parseHostAddress("10.33.5.22:33", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts.length).isEqualTo(2);
    assertThat(parts[0]).isEqualTo("10.33.5.22");
    assertThat(parts[1]).isEqualTo("33");
  }

  @Test
  public void testIPv6() {
    final String[] parts = HostUtil.parseHostAddress("fe80:0:0:0:250:56ff:fe9a:6990", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts.length).isEqualTo(2);
    assertThat(parts[0]).isEqualTo("fe80:0:0:0:250:56ff:fe9a:6990");
    assertThat(parts[1]).isEqualTo(HostUtil.CLIENT_DEFAULT_PORT);
  }

  @Test
  public void testIPv6WithPort() {
    final String[] parts = HostUtil.parseHostAddress("fe80:0:0:0:250:56ff:fe9a:6990:22", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts.length).isEqualTo(2);
    assertThat(parts[0]).isEqualTo("fe80:0:0:0:250:56ff:fe9a:6990");
    assertThat(parts[1]).isEqualTo("22");
  }
}
