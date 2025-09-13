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

import static java.util.Arrays.*;

public enum KeystoreType {

  PKCS12("PKCS12"),
  JKS("JKS");

  private final String keystoreType;

  KeystoreType(String keystoreType) {
    this.keystoreType = keystoreType;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public static KeystoreType validateFromString(final String keystoreType) {
    return stream(KeystoreType.values())
        .filter(enumValue -> enumValue.getKeystoreType().equalsIgnoreCase(keystoreType))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Specified key store type is not valid"));
  }

  public static KeystoreType getFromStringWithDefault(final String keystoreType,
                                                      final KeystoreType defaultKeystoreType) {
    return stream(KeystoreType.values())
        .filter(enumValue -> enumValue.getKeystoreType().equalsIgnoreCase(keystoreType))
        .findFirst()
        .orElse(defaultKeystoreType);
  }

}
