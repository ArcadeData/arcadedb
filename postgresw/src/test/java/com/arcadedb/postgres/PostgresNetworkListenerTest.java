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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PostgresNetworkListener, specifically the port parsing logic.
 */
class PostgresNetworkListenerTest {

  @Test
  void getPortsSinglePort() throws Exception {
    int[] ports = invokeGetPorts("5432");
    assertThat(ports).containsExactly(5432);
  }

  @Test
  void getPortsMultipleEnumeratedPorts() throws Exception {
    int[] ports = invokeGetPorts("5432,5433,5434");
    assertThat(ports).containsExactly(5432, 5433, 5434);
  }

  @Test
  void getPortsRangePorts() throws Exception {
    int[] ports = invokeGetPorts("5432-5435");
    assertThat(ports).containsExactly(5432, 5433, 5434, 5435);
  }

  @Test
  void getPortsSinglePortRange() throws Exception {
    int[] ports = invokeGetPorts("5432-5432");
    assertThat(ports).containsExactly(5432);
  }

  @Test
  void getPortsRangeWithTwoPorts() throws Exception {
    int[] ports = invokeGetPorts("5432-5433");
    assertThat(ports).containsExactly(5432, 5433);
  }

  @Test
  void getPortsEnumeratedTwoPorts() throws Exception {
    int[] ports = invokeGetPorts("5432,5434");
    assertThat(ports).containsExactly(5432, 5434);
  }

  @Test
  void getPortsHighPortNumber() throws Exception {
    int[] ports = invokeGetPorts("65535");
    assertThat(ports).containsExactly(65535);
  }

  @Test
  void getPortsLowPortNumber() throws Exception {
    int[] ports = invokeGetPorts("1");
    assertThat(ports).containsExactly(1);
  }

  @Test
  void getPortsRangeOfThree() throws Exception {
    int[] ports = invokeGetPorts("8080-8082");
    assertThat(ports).containsExactly(8080, 8081, 8082);
  }

  @Test
  void getPortsEnumeratedThreePorts() throws Exception {
    int[] ports = invokeGetPorts("80,443,8080");
    assertThat(ports).containsExactly(80, 443, 8080);
  }

  /**
   * Uses reflection to invoke the private static getPorts method.
   */
  private int[] invokeGetPorts(String portRange) throws Exception {
    Method method = PostgresNetworkListener.class.getDeclaredMethod("getPorts", String.class);
    method.setAccessible(true);
    return (int[]) method.invoke(null, portRange);
  }
}
