package com.arcadedb.server.http.ssl;

import static java.util.Arrays.stream;

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
