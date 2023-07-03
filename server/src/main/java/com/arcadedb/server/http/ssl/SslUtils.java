package com.arcadedb.server.http.ssl;

import com.arcadedb.server.security.ServerSecurityException;

import java.io.*;
import java.security.*;
import java.security.cert.*;
import java.util.function.*;

public class SslUtils {

  private static final String JAVAX_NET_SSL_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  private static final String JAVAX_NET_SSL_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";

  private SslUtils() {
  }

  public static KeyStore loadKeystoreFromStream(InputStream keystoreInputStream,
                                                String keystorePassword,
                                                String keystoreType)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {

    try (InputStream validatedInputStream = doValidateKeystoreStream(keystoreInputStream)) {
      KeyStore keystore = KeyStore.getInstance(keystoreType);
      keystore.load(validatedInputStream,
          keystorePassword.toCharArray());
      return keystore;
    }

  }

  public static String getDefaultKeystoreTypeForKeystore(Supplier<KeystoreType> defaultSupplier) {
    return doGetDefaultKeystoreType(JAVAX_NET_SSL_KEYSTORE_TYPE,
        defaultSupplier);
  }

  public static String getDefaultKeystoreTypeForTruststore(Supplier<KeystoreType> defaultSupplier) {
    return doGetDefaultKeystoreType(JAVAX_NET_SSL_TRUSTSTORE_TYPE,
        defaultSupplier);
  }

  private static String doGetDefaultKeystoreType(String propertyName,
                                                 Supplier<KeystoreType> defaultSupplier) {

    String keystoreTypeValue = System.getProperty(propertyName);
    // Keystore type is not defined take value from 'defaultSupplier'
    if ((keystoreTypeValue == null) || keystoreTypeValue.isEmpty()) {
      return doValidateDefaultKeystoreSupplier(defaultSupplier)
          .get()
          .getKeystoreType();
    }
    // Try to match with supported keystore types and return the first that matches
    // If no match return the value from 'defaultSupplier'
    return KeystoreType.getFromStringWithDefault(keystoreTypeValue,
            doValidateDefaultKeystoreSupplier(defaultSupplier).get())
        .getKeystoreType();

  }

  private static Supplier<KeystoreType> doValidateDefaultKeystoreSupplier(Supplier<KeystoreType> defaultSupplier) {
    if ((defaultSupplier == null) || (defaultSupplier.get() == null)) {
      throw new ServerSecurityException("Default key store supplier is not configured correctly");
    }
    return defaultSupplier;
  }

  private static InputStream doValidateKeystoreStream(InputStream keystoreInputStream) {
    if (keystoreInputStream == null) {
      throw new ServerSecurityException("Key store stream cannot be null");
    }
    return keystoreInputStream;
  }

}
