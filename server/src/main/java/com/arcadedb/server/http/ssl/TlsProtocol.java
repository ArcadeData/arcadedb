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
