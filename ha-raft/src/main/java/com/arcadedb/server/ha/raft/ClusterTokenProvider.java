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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.logging.Level;

/**
 * Derives and caches the cluster token used for inter-node authentication.
 * The token is either explicitly configured or derived from the cluster name
 * and root password using PBKDF2-HMAC-SHA256.
 */
class ClusterTokenProvider {

  // PBKDF2 parameters for cluster token derivation.
  // 100k iterations is the OWASP 2023 recommendation for PBKDF2-HMAC-SHA256.
  private static final int PBKDF2_ITERATIONS      = 100_000;
  private static final int PBKDF2_KEY_LENGTH_BITS = 256;

  private final    ContextConfiguration configuration;
  private volatile String               clusterToken;

  ClusterTokenProvider(final ContextConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Returns the cluster token, deriving it lazily on first call if not explicitly configured.
   */
  String getClusterToken() {
    if (clusterToken == null)
      initClusterToken();
    return clusterToken;
  }

  /**
   * Derives the cluster token eagerly. Called at startup to avoid blocking a request thread
   * with the expensive PBKDF2 computation.
   */
  synchronized void initClusterToken() {
    if (clusterToken != null)
      return;
    final String configured = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isEmpty()) {
      this.clusterToken = configured;
      return;
    }
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    if (clusterName == null || clusterName.isEmpty())
      throw new ConfigurationException(
          "Cannot derive cluster token: the cluster name is empty. Set arcadedb.ha.clusterName to a unique value or provide an explicit arcadedb.ha.clusterToken");
    // Check both the server's ContextConfiguration and the global default (system property)
    String rootPasswordStr = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (rootPasswordStr == null || rootPasswordStr.isEmpty())
      rootPasswordStr = GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();
    if (rootPasswordStr == null || rootPasswordStr.isEmpty())
      throw new ConfigurationException(
          "Cannot start HA mode without authentication: the auto-derived cluster token requires a root password. "
              + "Set arcadedb.server.rootPassword or provide an explicit arcadedb.ha.clusterToken");
    if ("production".equals(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE))
        && "arcadedb".equalsIgnoreCase(clusterName))
      LogManager.instance().log(this, Level.WARNING,
          "HA cluster is using the default cluster name '%s'. For stronger token domain separation, set arcadedb.ha.clusterName to a unique value or provide an explicit arcadedb.ha.clusterToken",
          clusterName);

    this.clusterToken = deriveTokenInternal(clusterName, rootPasswordStr);

    if ("production".equals(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE)))
      LogManager.instance().log(this, Level.WARNING,
          "Using auto-derived cluster token. Changing root password does NOT rotate this token. "
              + "To explicitly rotate, set arcadedb.ha.clusterToken=<new-value> and restart all nodes");
  }

  /**
   * Derives and stores the cluster token in {@code config} using the same PBKDF2 logic as the
   * instance {@link #initClusterToken()} method. Exposed for unit tests that cannot instantiate
   * a full {@link RaftHAServer}.
   * <p>
   * If {@link GlobalConfiguration#HA_CLUSTER_TOKEN} is already set in {@code config}, this
   * method is a no-op.
   */
  static void initClusterTokenForTest(final ContextConfiguration config) {
    final String configured = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isEmpty())
      return;

    final String clusterName = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    if (clusterName == null || clusterName.isEmpty())
      throw new ConfigurationException(
          "Cannot derive cluster token: the cluster name is empty. Set arcadedb.ha.clusterName to a unique value or provide an explicit arcadedb.ha.clusterToken");
    String rootPasswordStr = config.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (rootPasswordStr == null || rootPasswordStr.isEmpty())
      rootPasswordStr = GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();
    if (rootPasswordStr == null || rootPasswordStr.isEmpty())
      throw new ConfigurationException(
          "Cannot derive cluster token without a root password. Set arcadedb.server.rootPassword or arcadedb.ha.clusterToken");

    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, deriveTokenInternal(clusterName, rootPasswordStr));
  }

  /**
   * Converts the root password String to a char[], delegates to
   * {@link #deriveTokenFromPassword(String, char[])}, and zeros the array before returning.
   */
  private static String deriveTokenInternal(final String clusterName, final String rootPassword) {
    final char[] pw = rootPassword.toCharArray();
    try {
      return deriveTokenFromPassword(clusterName, pw);
    } finally {
      Arrays.fill(pw, '\0');
    }
  }

  /**
   * PBKDF2-HMAC-SHA256 derivation of a cluster token from a cluster name and root password.
   * Domain separation: the cluster name appears in both the password and the salt so that
   * two clusters with the same root password produce different tokens.
   *
   * <p>The caller's {@code rootPassword} array is NOT zeroed here (callers own their copy).
   * All intermediate password material created inside this method is zeroed before returning.
   */
  static String deriveTokenFromPassword(final String clusterName, final char[] rootPassword) {
    // Build "clusterName:rootPassword" as a char[] so we can zero it after use.
    final char[] clusterChars = clusterName.toCharArray();
    final char[] passwordChars = new char[clusterChars.length + 1 + rootPassword.length];
    System.arraycopy(clusterChars, 0, passwordChars, 0, clusterChars.length);
    passwordChars[clusterChars.length] = ':';
    System.arraycopy(rootPassword, 0, passwordChars, clusterChars.length + 1, rootPassword.length);

    try {
      final byte[] salt = ("arcadedb-cluster-token:" + clusterName).getBytes(StandardCharsets.UTF_8);
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      final PBEKeySpec spec = new PBEKeySpec(passwordChars, salt, PBKDF2_ITERATIONS, PBKDF2_KEY_LENGTH_BITS);
      final byte[] hash = factory.generateSecret(spec).getEncoded();
      spec.clearPassword();
      return HexFormat.of().formatHex(hash);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to derive cluster token", e);
    } finally {
      Arrays.fill(passwordChars, '\0');
      Arrays.fill(clusterChars, '\0');
    }
  }
}
