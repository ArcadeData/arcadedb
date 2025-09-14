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
package com.arcadedb.server.http.ssl;

public enum TlsProtocol {

  TLS("TLS"), // Supports some version of TLS
  TLS_1("TLSv1"), // Supports RFC 2246: TLS version 1.0
  TLS_1_1("TLSv1.1"), // Supports RFC 4346: TLS version 1.1
  TLS_1_2("TLSv1.2"), // Supports RFC 5246: TLS version 1.2
  TLS_1_3("TLSv1.3"); // Supports RFC 8446: TLS version 1.3

  private final String tlsVersion;

  TlsProtocol(String tlsVersion) {
    this.tlsVersion = tlsVersion;
  }

  public String getTlsVersion() {
    return tlsVersion;
  }

  public static TlsProtocol getLatestTlsVersion() {
    return TLS_1_3;
  }

}
