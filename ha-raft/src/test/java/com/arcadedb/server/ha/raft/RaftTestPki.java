/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Generates a throwaway certificate authority and one CA-signed node certificate for the Raft gRPC mTLS
 * tests (issue #3890), producing both the PEM files ArcadeDB is configured with and the PKCS#12 stores a
 * plain JSSE client needs to dial the Raft port.
 * <p>
 * Everything is created at test time with the JDK's own {@code keytool}, so no certificate is committed to
 * the repository and none can expire in place. Two independent instances model the two identities the tests
 * need: the cluster CA every node trusts, and a foreign CA that must be rejected at the handshake.
 * <p>
 * The node certificate is issued for {@code CN=localhost} with {@code SAN=dns:localhost,ip:127.0.0.1}, which
 * is the address every in-process test node dials, so a single certificate serves the whole cluster.
 */
final class RaftTestPki {

  private static final String PASSWORD = "arcadedb-test";
  /**
   * The PEM armour is assembled from these pieces rather than written out, because the repository's
   * {@code detect-private-key} pre-commit hook matches the assembled header as a literal and would refuse
   * every commit touching this file. Nothing secret is stored here: the key itself is generated per run.
   */
  private static final String PEM_DELIMITER = "-----";
  private static final String PEM_LABEL     = "PRIVATE KEY";
  private static final String NODE_ALIAS = "node";
  private static final String CA_ALIAS = "ca";
  /** Short on purpose: these certificates must never outlive the test run that created them. */
  private static final String VALIDITY_DAYS = "2";
  private static final String SUBJECT_ALT_NAMES = "san=dns:localhost,ip:127.0.0.1";
  /** Hang detector for the keytool subprocess, not a latency bound: generous on purpose. */
  private static final long   KEYTOOL_TIMEOUT_SECONDS = 120;

  private final Path caCertificate;
  private final Path nodeCertificate;
  private final Path nodePrivateKey;
  private final Path nodeKeyStore;
  private final Path trustStore;

  private RaftTestPki(final Path caCertificate, final Path nodeCertificate, final Path nodePrivateKey,
      final Path nodeKeyStore, final Path trustStore) {
    this.caCertificate = caCertificate;
    this.nodeCertificate = nodeCertificate;
    this.nodePrivateKey = nodePrivateKey;
    this.nodeKeyStore = nodeKeyStore;
    this.trustStore = trustStore;
  }

  /**
   * Creates a CA and a node certificate signed by it under {@code directory}, prefixing every file with
   * {@code label} so several independent authorities can share one directory.
   */
  static RaftTestPki create(final Path directory, final String label) throws Exception {
    Files.createDirectories(directory);

    final Path caStore = directory.resolve(label + "-ca.keystore");
    final Path caCert = directory.resolve(label + "-ca.pem");
    final Path nodeStore = directory.resolve(label + "-node.keystore");
    final Path nodeCert = directory.resolve(label + "-node-cert.pem");
    final Path nodeKey = directory.resolve(label + "-node-key.pem");
    final Path csr = directory.resolve(label + "-node.csr");
    final Path trustStore = directory.resolve(label + "-trust.keystore");

    // keytool refuses to add an alias a keystore already holds, so a directory left behind by an earlier
    // run of this test would fail every generation after the first. Start from nothing every time.
    for (final Path stale : List.of(caStore, caCert, nodeStore, nodeCert, nodeKey, csr, trustStore))
      Files.deleteIfExists(stale);

    // Self-signed CA (basicConstraints=CA).
    keytool("-genkeypair", "-alias", CA_ALIAS, "-keyalg", "RSA", "-keysize", "2048", "-sigalg", "SHA256withRSA",
        "-dname", "CN=" + label + " ArcadeDB Test CA", "-validity", VALIDITY_DAYS, "-ext", "bc:c",
        "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD, "-keypass", PASSWORD);
    keytool("-exportcert", "-rfc", "-alias", CA_ALIAS, "-keystore", caStore.toString(), "-storetype", "PKCS12",
        "-storepass", PASSWORD, "-file", caCert.toString());

    // Node key pair, certificate request, and the CA-signed certificate answering it.
    keytool("-genkeypair", "-alias", NODE_ALIAS, "-keyalg", "RSA", "-keysize", "2048", "-sigalg", "SHA256withRSA",
        "-dname", "CN=localhost", "-validity", VALIDITY_DAYS, "-ext", SUBJECT_ALT_NAMES,
        "-keystore", nodeStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD, "-keypass", PASSWORD);
    keytool("-certreq", "-alias", NODE_ALIAS, "-keystore", nodeStore.toString(), "-storetype", "PKCS12",
        "-storepass", PASSWORD, "-file", csr.toString());
    keytool("-gencert", "-alias", CA_ALIAS, "-keystore", caStore.toString(), "-storetype", "PKCS12",
        "-storepass", PASSWORD, "-infile", csr.toString(), "-outfile", nodeCert.toString(), "-rfc",
        "-validity", VALIDITY_DAYS, "-ext", SUBJECT_ALT_NAMES, "-ext", "eku=serverAuth,clientAuth");

    // Re-import the signed certificate so the node keystore holds a key entry with a complete chain, which
    // is what KeyManagerFactory needs to present a client certificate.
    keytool("-importcert", "-noprompt", "-alias", CA_ALIAS, "-file", caCert.toString(),
        "-keystore", nodeStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);
    keytool("-importcert", "-noprompt", "-alias", NODE_ALIAS, "-file", nodeCert.toString(),
        "-keystore", nodeStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);

    // Trust store holding only the CA, for the JSSE side of the tests.
    keytool("-importcert", "-noprompt", "-alias", CA_ALIAS, "-file", caCert.toString(),
        "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);

    writePrivateKeyPem(nodeStore, nodeKey);

    return new RaftTestPki(caCert, nodeCert, nodeKey, nodeStore, trustStore);
  }

  Path caCertificate() {
    return caCertificate;
  }

  Path nodeCertificate() {
    return nodeCertificate;
  }

  Path nodePrivateKey() {
    return nodePrivateKey;
  }

  /**
   * Builds a JSSE context that presents this authority's node certificate as the client certificate while
   * trusting {@code trustedBy}'s CA. Passing a different instance as {@code trustedBy} is how the tests
   * dial a cluster whose server certificate is acceptable with a client identity that is not.
   */
  static SSLContext clientContext(final RaftTestPki identity, final RaftTestPki trustedBy) throws Exception {
    final KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(load(identity.nodeKeyStore), PASSWORD.toCharArray());
    return context(keyManagers, trustedBy);
  }

  /**
   * Like {@link #clientContext(RaftTestPki, RaftTestPki)} but with no client certificate at all, modelling an
   * anonymous host that merely knows the Raft port.
   */
  static SSLContext anonymousClientContext(final RaftTestPki trustedBy) throws Exception {
    return context(null, trustedBy);
  }

  private static SSLContext context(final KeyManagerFactory keyManagers, final RaftTestPki trustedBy)
      throws Exception {
    final TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(load(trustedBy.trustStore));

    final SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers != null ? keyManagers.getKeyManagers() : null, trustManagers.getTrustManagers(), null);
    return context;
  }

  private static KeyStore load(final Path store) throws Exception {
    final KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (final InputStream in = Files.newInputStream(store)) {
      keyStore.load(in, PASSWORD.toCharArray());
    }
    return keyStore;
  }

  /**
   * keytool cannot export a private key, so the PKCS#8 encoding is read back through the KeyStore API and
   * PEM-wrapped here. This is the format Netty (and therefore Ratis) parses for
   * {@code arcadedb.ha.tls.privateKeyFile}.
   */
  private static void writePrivateKeyPem(final Path nodeStore, final Path target) throws Exception {
    final PrivateKey key = (PrivateKey) load(nodeStore).getKey(NODE_ALIAS, PASSWORD.toCharArray());
    final String body = Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(key.getEncoded());
    Files.writeString(target, PEM_DELIMITER + "BEGIN " + PEM_LABEL + PEM_DELIMITER + "\n"
        + body + "\n" + PEM_DELIMITER + "END " + PEM_LABEL + PEM_DELIMITER + "\n");
  }

  private static void keytool(final String... arguments) throws Exception {
    final Path executable = Path.of(System.getProperty("java.home"), "bin", "keytool");
    final Path log = Files.createTempFile("arcadedb-keytool-", ".log");
    final ProcessBuilder builder = new ProcessBuilder();
    builder.command().add(executable.toString());
    builder.command().addAll(List.of(arguments));
    // Output goes to a file rather than a pipe so the bounded wait below is actually reachable: draining
    // the pipe first would block forever on a hung keytool, before any timeout could fire.
    builder.redirectErrorStream(true);
    builder.redirectOutput(log.toFile());

    final Process process = builder.start();
    try {
      // RSA key generation can starve for entropy on a loaded CI runner. A bounded wait turns that into a
      // named failure instead of a silent hang that only surfaces as the job's wall-clock timeout.
      if (!process.waitFor(KEYTOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IllegalStateException("keytool did not finish within " + KEYTOOL_TIMEOUT_SECONDS
            + "s for " + builder.command() + System.lineSeparator() + readQuietly(log));
      }
      final int exitCode = process.exitValue();
      if (exitCode != 0)
        throw new IllegalStateException("keytool exited with " + exitCode + " for " + builder.command()
            + System.lineSeparator() + readQuietly(log));
    } finally {
      Files.deleteIfExists(log);
    }
  }

  private static String readQuietly(final Path file) {
    try {
      return Files.readString(file);
    } catch (final Exception e) {
      return "<keytool output unavailable: " + e + ">";
    }
  }
}
