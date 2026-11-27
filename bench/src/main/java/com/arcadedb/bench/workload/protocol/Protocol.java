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
package com.arcadedb.bench.workload.protocol;

import java.util.Locale;

/**
 * A wire protocol an openCypher client can reach the server under test through. The case name is the form used in benchmark
 * case names ({@code http}, {@code grpc}, {@code bolt}, {@code pg}).
 */
public enum Protocol {
  HTTP("http"), GRPC("grpc"), BOLT("bolt"), POSTGRES("pg");

  private final String caseName;

  Protocol(final String caseName) {
    this.caseName = caseName;
  }

  public String caseName() {
    return caseName;
  }

  /** The protocol of a case name, any case. */
  public static Protocol ofCaseName(final String caseName) {
    final String name = caseName.toLowerCase(Locale.ROOT);
    for (final Protocol p : values())
      if (p.caseName.equals(name))
        return p;
    throw new IllegalArgumentException("Unknown protocol '" + caseName + "', expected one of http, grpc, bolt, pg");
  }
}
