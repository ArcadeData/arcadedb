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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Derives and caches the cluster token used for inter-node authentication.
 * The token is either explicitly configured or derived from the cluster name
 * and root password using PBKDF2-HMAC-SHA256. The root password is taken inline or,
 * failing that, from the file at {@code arcadedb.server.rootPasswordPath}.
 */
class ClusterTokenProvider {

  // PBKDF2 parameters for cluster token derivation.
  // 100k iterations is the OWASP 2023 recommendation for PBKDF2-HMAC-SHA256.
  private static final int PBKDF2_ITERATIONS      = 100_000;
  private static final int PBKDF2_KEY_LENGTH_BITS = 256;

  /**
   * At most one refusal report per this many milliseconds (issue #10601): a peer on another token is refused on every
   * hop it makes, and one line naming the cause is enough.
   */
  static final long REFUSAL_REPORT_INTERVAL_MS = 60_000L;

  /** Where the token this node authenticates with came from (issue #10601). */
  enum Source {
    /** {@code arcadedb.ha.clusterToken}. */
    EXPLICIT,
    /** The file at {@code arcadedb.ha.clusterTokenPath}. */
    EXPLICIT_FILE,
    /** Derived from {@code arcadedb.ha.clusterName} and {@code arcadedb.server.rootPassword}. */
    DERIVED_INLINE,
    /** Derived from {@code arcadedb.ha.clusterName} and the file at {@code arcadedb.server.rootPasswordPath}. */
    DERIVED_FILE
  }

  private final    ContextConfiguration configuration;
  private final    AtomicLong           lastRefusalReportMs = new AtomicLong(Long.MIN_VALUE);
  private volatile String               clusterToken;
  private volatile Source               source;
  private volatile String               clusterName;
  // Only for Source.DERIVED_FILE: the path the password was read from, and the SHA-256 of what was read there, so a
  // refusal can tell whether the file has been rewritten since this node derived its token.
  private volatile String               rootPasswordPath;
  private volatile byte[]               rootPasswordDigest;

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
    final String resolved = resolveExplicitToken(configuration);
    if (resolved != null) {
      final String inline = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
      this.source = inline != null && !inline.isEmpty() ? Source.EXPLICIT : Source.EXPLICIT_FILE;
      this.clusterToken = resolved;
      return;
    }
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    if (clusterName == null || clusterName.isEmpty())
      throw new ConfigurationException(
          "Cannot derive cluster token: the cluster name is empty. Set arcadedb.ha.clusterName to a unique value or provide an explicit arcadedb.ha.clusterToken");
    final String rootPasswordStr = resolveRootPassword(configuration);
    if (rootPasswordStr == null)
      throw new ConfigurationException(
          """
          Cannot start HA mode without authentication: the auto-derived cluster token requires a root password. \
          Set arcadedb.server.rootPassword or arcadedb.server.rootPasswordPath, or provide an explicit \
          arcadedb.ha.clusterToken or arcadedb.ha.clusterTokenPath""");
    if ("production".equals(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE))
        && "arcadedb".equalsIgnoreCase(clusterName))
      LogManager.instance().log(this, Level.WARNING,
          "HA cluster is using the default cluster name '%s'. For stronger token domain separation, set arcadedb.ha.clusterName to a unique value or provide an explicit arcadedb.ha.clusterToken",
          clusterName);

    this.clusterName = clusterName;
    if (nonEmptyValue(configuration, GlobalConfiguration.SERVER_ROOT_PASSWORD) != null)
      this.source = Source.DERIVED_INLINE;
    else {
      this.source = Source.DERIVED_FILE;
      this.rootPasswordPath = nonEmptyValue(configuration, GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH);
      this.rootPasswordDigest = sha256(rootPasswordStr);
    }
    this.clusterToken = deriveTokenInternal(clusterName, rootPasswordStr);

    if ("production".equals(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE)))
      LogManager.instance().log(this, Level.WARNING, derivedTokenWarning(source));
  }

  /**
   * The startup warning of a node whose token is derived (issue #10601). Before it, the warning said that changing the
   * root password does NOT rotate the token, which is the opposite of what happens: the token is derived from the
   * password at every start and never stored, so a changed password - including a file at
   * {@code arcadedb.server.rootPasswordPath} rewritten in place - is a changed token at the next start of each node.
   */
  static String derivedTokenWarning(final Source source) {
    final String passwordSource = source == Source.DERIVED_FILE ?
        "the content of the file at arcadedb.server.rootPasswordPath" :
        "arcadedb.server.rootPassword";
    return "Using a cluster token derived from arcadedb.ha.clusterName and the root password (" + passwordSource
        + "). The token is derived again at every start and never stored: changing either input - including rewriting "
        + "the root password file in place, as rotating a mounted Kubernetes Secret does - changes the token at the next "
        + "start of each node, and nodes holding different tokens refuse each other's requests with 401 'Invalid cluster "
        + "token'. Rewriting the file does not change the password of an existing root user either. To decouple the "
        + "token from the root password, set arcadedb.ha.clusterToken or arcadedb.ha.clusterTokenPath to the same value "
        + "on every node and restart all nodes";
  }

  /**
   * Where this node's token came from, as an operator reads it in the log: the settings, and for a derived token the
   * cluster name and the password file path. Never the token or the password. Null before the token is resolved.
   */
  String describeSource() {
    final Source s = source;
    if (s == null)
      return null;
    return switch (s) {
      case EXPLICIT -> "set explicitly by arcadedb.ha.clusterToken";
      case EXPLICIT_FILE -> "read from the file at arcadedb.ha.clusterTokenPath ('"
          + configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH) + "')";
      case DERIVED_INLINE -> "derived from arcadedb.ha.clusterName ('" + clusterName
          + "') and the root password set by arcadedb.server.rootPassword";
      case DERIVED_FILE -> "derived from arcadedb.ha.clusterName ('" + clusterName
          + "') and the root password read from the file at arcadedb.server.rootPasswordPath ('" + rootPasswordPath
          + "') when this node started";
    };
  }

  Source getSource() {
    return source;
  }

  /**
   * Whether the file this node's token was derived from no longer holds the password it held when the token was derived
   * (issue #10601), which is the moment the cause of a token mismatch is still visible: a rotated Secret. False for any
   * token not derived from a file. A file that can no longer be read counts as changed: this node cannot confirm it.
   */
  boolean rootPasswordFileChangedSinceDerivation() {
    return rootPasswordFileChange() != null;
  }

  /**
   * The sentence a refusal report adds about the root password file, or null when the token is not derived from a file
   * or the file still holds the password the token was derived from. A file that cannot be read is reported as such,
   * not as rewritten: it may be only temporarily unreadable.
   */
  private String rootPasswordFileChange() {
    if (source != Source.DERIVED_FILE)
      return null;
    final String current;
    try {
      current = ServerSecurity.readRootPasswordFile(rootPasswordPath);
    } catch (final ServerSecurityException e) {
      return " The file at arcadedb.server.rootPasswordPath ('" + rootPasswordPath + "') can no longer be read (" + e.getMessage()
          + "), so this node cannot confirm it still holds the password its token was derived from";
    }
    if (MessageDigest.isEqual(rootPasswordDigest, sha256(current)))
      return null;
    return " The file at arcadedb.server.rootPasswordPath ('" + rootPasswordPath
        + "') no longer holds the password this node derived its token from: it has been rewritten since this node "
        + "started (e.g. a rotated Kubernetes Secret), so nodes started before and after the rewrite hold different "
        + "tokens. Restore the previous password, or set arcadedb.ha.clusterToken or arcadedb.ha.clusterTokenPath "
        + "explicitly on every node and restart all nodes";
  }

  /**
   * Builds the line a refused cluster-token hop logs on this node (issue #10601): that the caller's token does not
   * match this node's, where this node's came from, and - for a token derived from a since-rewritten file - that the
   * rewrite is the likely cause. Worded for every caller of the hook: on the snapshot endpoint a request whose token is
   * refused may still authenticate with Basic credentials, so the line speaks of the token, not of the request.
   */
  String clusterTokenRefusalReport(final String caller) {
    final StringBuilder sb = new StringBuilder("Refused the cluster token of a request from ")
        .append(caller != null ? caller : "an unknown caller")
        .append(": the caller's cluster token does not match this node's, which was ")
        .append(describeSource())
        .append('.');
    final String fileChange = rootPasswordFileChange();
    if (fileChange != null)
      sb.append(fileChange);
    else if (source == Source.DERIVED_INLINE || source == Source.DERIVED_FILE)
      sb.append(" The caller may run with a different arcadedb.ha.clusterName or root password");
    else
      sb.append(" The caller may run with a different explicit cluster token");
    return sb.toString();
  }

  /**
   * Logs a refused cluster-token hop at WARNING, at most once per {@link #REFUSAL_REPORT_INTERVAL_MS} so a peer on
   * another token cannot flood the log (or make this node re-read the password file) with one line per request.
   * <p>
   * Callers reach this before the request is authenticated (the token check IS the authentication, in
   * {@code AbstractServerHttpHandler} and {@code SnapshotHttpHandler}), so any client can trigger it: the interval gate
   * is what bounds the file read and the log line an unauthenticated caller can cause, and must stay in front of both.
   * The gate is one per node, not one per caller: a client sending bad tokens can use the window up and hide the line a
   * misconfigured peer would have produced in it. The next window reports again, which is enough for a peer that keeps
   * being refused, and a per-caller map would be state an unauthenticated caller could grow.
   *
   * @return whether this call logged
   */
  boolean reportClusterTokenRefusal(final String caller, final long nowMs) {
    final long last = lastRefusalReportMs.get();
    if (last != Long.MIN_VALUE && nowMs - last < REFUSAL_REPORT_INTERVAL_MS)
      return false;
    if (!lastRefusalReportMs.compareAndSet(last, nowMs))
      return false;
    LogManager.instance().log(this, Level.WARNING, clusterTokenRefusalReport(caller));
    return true;
  }

  /**
   * Holds a node whose token is derived from the root password file to the token it last ran with (issue #10601).
   * <p>
   * The derived token is never stored, so a file rewritten in place (a rotated Kubernetes Secret) makes each node pick
   * up a new token at its next restart, one pod at a time, and a rolling restart leaves the two halves of the cluster
   * answering each other 401. This records a fingerprint of the derived token in {@code fingerprintFile} - a sibling of
   * the Raft storage directory - and refuses to start when the file now derives a different one, unless
   * {@code allowRotation} says the operator is rotating deliberately. The fingerprint is a SHA-256 of the token, never
   * the token or the password.
   * <p>
   * A token that is not derived from a file has nothing to hold: the record is removed, so switching back to a file
   * later starts from that file rather than from a stale record.
   *
   * @throws ConfigurationException when the recorded fingerprint differs and rotation is not allowed
   */
  void verifyDerivedTokenFingerprint(final File fingerprintFile, final boolean allowRotation) {
    final Path path = fingerprintFile.toPath();
    try {
      if (getSource() != Source.DERIVED_FILE) {
        Files.deleteIfExists(path);
        return;
      }
      final String fingerprint = tokenFingerprint(getClusterToken());
      // An empty record (a crash mid-write where the move below cannot be atomic, or a file emptied by hand) records no
      // token, so it is treated as absent rather than as a rotation.
      final String recorded = Files.exists(path) ? Files.readString(path, StandardCharsets.UTF_8).strip() : "";
      if (!recorded.isEmpty()) {
        if (recorded.equals(fingerprint)) {
          if (allowRotation)
            LogManager.instance().log(this, Level.WARNING,
                "arcadedb.ha.clusterTokenAllowRotation is set and the derived cluster token has not changed: remove the "
                    + "setting once every node runs on the rotated token, as it keeps the check against an in-place rewrite of "
                    + "'%s' off for every start", rootPasswordPath);
          return;
        }
        if (!allowRotation)
          throw new ConfigurationException("""
              Refusing to start: the cluster token this node derives from arcadedb.ha.clusterName and the root password \
              file at arcadedb.server.rootPasswordPath ('%s') differs from the one it last ran with (recorded in '%s'). \
              The file has been rewritten since this node last started (e.g. a rotated Kubernetes Secret); a node starting \
              with the new token is refused with 401 'Invalid cluster token' by every peer still holding the old one. \
              Restore the previous password, or rotate deliberately: set arcadedb.ha.clusterTokenAllowRotation=true on \
              every node and restart all of them together, or decouple the token from the root password with an explicit \
              arcadedb.ha.clusterToken or arcadedb.ha.clusterTokenPath""".formatted(rootPasswordPath,
              fingerprintFile.getAbsolutePath()));
        LogManager.instance().log(this, Level.WARNING,
            "The cluster token derived from the root password file at '%s' changed since this node last started, and "
                + "arcadedb.ha.clusterTokenAllowRotation is set: starting with the new token. Every node must restart with the "
                + "new password, or the nodes still on the old token refuse this one", rootPasswordPath);
      }
      // Recorded before the rest of the start runs: the token on disk is the one this node is about to use, so a start
      // that fails later still leaves the record describing the token it would come back with. Note that leaving
      // arcadedb.ha.clusterTokenAllowRotation on keeps this guard off for every later start.
      // On the first start of a node the Raft storage, and so the directory this record sits in, does not exist yet.
      final Path parent = path.toAbsolutePath().getParent();
      if (parent != null)
        Files.createDirectories(parent);
      // Written to a temporary file and moved over the record, so a crash mid-write never leaves a partial record that
      // the next start would read as a rotated password.
      final Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
      Files.writeString(tmp, fingerprint, StandardCharsets.UTF_8);
      try {
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final IOException e) {
      throw new ConfigurationException(
          "Cannot read or write the cluster token fingerprint at '" + fingerprintFile.getAbsolutePath() + "'", e);
    }
  }

  /** A SHA-256 of the token under a fixed domain prefix: identifies a token without being usable as one. */
  static String tokenFingerprint(final String token) {
    return HexFormat.of().formatHex(sha256("arcadedb-cluster-token-fingerprint:" + token));
  }

  private static byte[] sha256(final String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
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
    final String resolved = resolveExplicitToken(config);
    if (resolved != null) {
      config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, resolved);
      return;
    }

    final String clusterName = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    if (clusterName == null || clusterName.isEmpty())
      throw new ConfigurationException(
          "Cannot derive cluster token: the cluster name is empty. Set arcadedb.ha.clusterName to a unique value or provide an explicit arcadedb.ha.clusterToken");
    final String rootPasswordStr = resolveRootPassword(config);
    if (rootPasswordStr == null)
      throw new ConfigurationException(
          "Cannot derive cluster token without a root password. Set arcadedb.server.rootPassword, arcadedb.server.rootPasswordPath, arcadedb.ha.clusterToken or arcadedb.ha.clusterTokenPath");

    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, deriveTokenInternal(clusterName, rootPasswordStr));
  }

  /**
   * Resolves an explicitly provided cluster token, in precedence order:
   * <ol>
   *   <li>{@link GlobalConfiguration#HA_CLUSTER_TOKEN} if set;</li>
   *   <li>the trimmed content of the file at {@link GlobalConfiguration#HA_CLUSTER_TOKEN_PATH} if set.</li>
   * </ol>
   * Returns {@code null} when neither is configured, signalling that the token must be derived from
   * the cluster name and root password. Reading the secret from a file keeps it off the command line
   * (e.g. a Kubernetes Secret mounted on tmpfs); the content is trimmed because most tooling appends a
   * trailing newline, which would otherwise break the constant-time token comparison between nodes.
   *
   * @throws ConfigurationException if the token-path file is configured but cannot be read, or is empty.
   */
  private static String resolveExplicitToken(final ContextConfiguration config) {
    final String configured = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isEmpty())
      return configured;

    final String tokenPath = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN_PATH);
    if (tokenPath == null || tokenPath.isEmpty())
      return null;

    final Path path = Path.of(tokenPath);
    if (!Files.isReadable(path))
      throw new ConfigurationException("Error reading cluster token file at path '" + tokenPath + "'");
    final String token;
    try {
      token = Files.readString(path).strip();
    } catch (final IOException e) {
      throw new ConfigurationException("Error reading cluster token file at path '" + tokenPath + "'", e);
    }
    if (token.isEmpty())
      throw new ConfigurationException("Cluster token file at path '" + tokenPath + "' is empty");
    return token;
  }

  /**
   * Resolves the root password the token is derived from, with the same precedence as
   * {@code ServerSecurity.askForRootPassword()} uses to create the root user:
   * <ol>
   *   <li>{@link GlobalConfiguration#SERVER_ROOT_PASSWORD}, from the server's configuration, then from the global one;</li>
   *   <li>otherwise the content of the file at {@link GlobalConfiguration#SERVER_ROOT_PASSWORD_PATH}, looked up the same
   *   way, read by {@link ServerSecurity#readRootPasswordFile(String)}.</li>
   * </ol>
   * The file is read with the very same normalization as the root user's password, so a node configured with the
   * password inline and a node reading it from a file derive the same token (issue #10462).
   *
   * @return the root password, or {@code null} when neither setting is configured
   *
   * @throws ConfigurationException if the password file is configured but cannot be read, or is empty.
   */
  private static String resolveRootPassword(final ContextConfiguration config) {
    final String inline = nonEmptyValue(config, GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (inline != null)
      return inline;

    final String passwordPath = nonEmptyValue(config, GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH);
    if (passwordPath == null)
      return null;

    try {
      return ServerSecurity.readRootPasswordFile(passwordPath);
    } catch (final ServerSecurityException e) {
      throw new ConfigurationException("Cannot derive cluster token from the root password file: " + e.getMessage(), e);
    }
  }

  /**
   * Returns the setting from the server's configuration, falling back to the global default (system property), or
   * {@code null} when neither carries a non-empty value.
   */
  private static String nonEmptyValue(final ContextConfiguration config, final GlobalConfiguration setting) {
    String value = config.getValueAsString(setting);
    if (value == null || value.isEmpty())
      value = setting.getValueAsString();
    return value == null || value.isEmpty() ? null : value;
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
