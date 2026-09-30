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
package com.arcadedb.server.support;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.SecurityUserFileRepository;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Locale;
import java.util.logging.Level;

/**
 * The registration of this server with the ArcadeData customer portal: portal URL, Client ID and Client key. It is
 * stored in the file {@value #FILE_NAME} of the server configuration directory (owner-only permissions, atomic write) and
 * can be overridden by the settings {@code arcadedb.support.url}, {@code arcadedb.support.clientId} and
 * {@code arcadedb.support.clientKey} (containers, Kubernetes secrets).
 * <p>
 * The key is a credential: it is never returned by any API and never logged (only its last four characters), not even in
 * exception messages or in {@link Registration#toString()}.
 */
public class SupportConfiguration {
  public static final String FILE_NAME          = "support.json";
  public static final String DEFAULT_PORTAL_URL = GlobalConfiguration.SUPPORT_URL.getDefValue().toString();

  private static final int MAX_CLIENT_ID_LENGTH = 100;
  private static final int MAX_KEY_LENGTH       = 256;
  private static final int MIN_KEY_LENGTH       = 8;

  private final Path                 configDirectory;
  private final ContextConfiguration configuration;

  /**
   * The effective registration. The key is reachable only from this package (the portal client): nothing that leaves the
   * server can carry it.
   */
  public static final class Registration {
    private final String  portalUrl;
    private final String  clientId;
    private final String  key;
    private final String  registeredAt;
    private final boolean fromSettings;

    Registration(final String portalUrl, final String clientId, final String key, final String registeredAt,
        final boolean fromSettings) {
      this.portalUrl = portalUrl;
      this.clientId = clientId;
      this.key = key;
      this.registeredAt = registeredAt;
      this.fromSettings = fromSettings;
    }

    public String getPortalUrl() {
      return portalUrl;
    }

    public String getClientId() {
      return clientId;
    }

    public String getRegisteredAt() {
      return registeredAt;
    }

    /** True when the Client ID and key come from the settings and not from the file: Studio cannot unregister them. */
    public boolean isFromSettings() {
      return fromSettings;
    }

    /** The last four characters of the key, the only part of it that is ever shown. */
    public String getKeyHint() {
      return keyHint(key);
    }

    String getKey() {
      return key;
    }

    @Override
    public String toString() {
      return "Registration{portalUrl=" + portalUrl + ", clientId=" + clientId + ", key=" + keyHint(key) + "}";
    }
  }

  public SupportConfiguration(final Path configDirectory, final ContextConfiguration configuration) {
    this.configDirectory = configDirectory;
    this.configuration = configuration;
  }

  public static String keyHint(final String key) {
    if (key == null || key.isEmpty())
      return "";
    return "…" + (key.length() <= 4 ? "" : key.substring(key.length() - 4));
  }

  /** @return the effective registration, or {@code null} when the server is not registered */
  public synchronized Registration get() {
    final JSONObject file = readFile();

    final String settingId = trim(configuration.getValueAsString(GlobalConfiguration.SUPPORT_CLIENT_ID));
    final String settingKey = trim(configuration.getValueAsString(GlobalConfiguration.SUPPORT_CLIENT_KEY));
    final String settingUrl = trim(configuration.getValueAsString(GlobalConfiguration.SUPPORT_URL));

    final String clientId = !settingId.isEmpty() ? settingId : file.getString("clientId", "");
    final String key = !settingKey.isEmpty() ? settingKey : file.getString("key", "");
    if (clientId.isEmpty() || key.isEmpty())
      return null;

    final String url;
    if (!settingUrl.isEmpty() && !settingUrl.equals(DEFAULT_PORTAL_URL))
      url = settingUrl;
    else
      url = file.getString("portalUrl", DEFAULT_PORTAL_URL);

    return new Registration(stripTrailingSlash(url), clientId, key, file.getString("registeredAt", ""),
        !settingId.isEmpty() && !settingKey.isEmpty());
  }

  /** The portal URL to use, also when the server is not registered yet (the registration form calls it first). */
  public synchronized String getPortalUrl() {
    final Registration registration = get();
    if (registration != null)
      return registration.getPortalUrl();
    final String settingUrl = trim(configuration.getValueAsString(GlobalConfiguration.SUPPORT_URL));
    return stripTrailingSlash(settingUrl.isEmpty() ? DEFAULT_PORTAL_URL : settingUrl);
  }

  /** Whether the configuration directory accepts the file: false on a read-only volume. */
  public boolean canWriteConfig() {
    try {
      final Path file = configDirectory.resolve(FILE_NAME);
      if (Files.exists(file))
        return Files.isWritable(file) && Files.isWritable(configDirectory);
      return Files.isDirectory(configDirectory) ? Files.isWritable(configDirectory) : firstExistingParentWritable(configDirectory);
    } catch (final RuntimeException e) {
      return false;
    }
  }

  /**
   * Stores a registration in {@value #FILE_NAME} (atomic write, owner-only permissions).
   *
   * @throws IllegalArgumentException when the Client ID, the key or the URL is not acceptable
   * @throws IOException              when the file cannot be written
   */
  public synchronized void save(final String portalUrl, final String clientId, final String key) throws IOException {
    validateClientId(clientId);
    validateKey(key);
    final String url = stripTrailingSlash(portalUrl == null || portalUrl.isBlank() ? DEFAULT_PORTAL_URL : portalUrl.trim());
    validatePortalUrl(url);

    final JSONObject json = new JSONObject();
    json.put("portalUrl", url);
    json.put("clientId", clientId.trim());
    json.put("key", key.trim());
    json.put("registeredAt", Instant.now().toString());

    final Path file = configDirectory.resolve(FILE_NAME);
    FileUtils.atomicWriteFile(file.toFile(), json.toString(2));
    SecurityUserFileRepository.applyOwnerOnlyPermissions(file);
    LogManager.instance().log(this, Level.INFO, "Support registration saved for client '%s' (key %s)", clientId.trim(),
        keyHint(key.trim()));
  }

  /**
   * Removes the registration file.
   *
   * @throws IllegalStateException when the registration comes from the settings, that Studio cannot change
   */
  public synchronized void clear() throws IOException {
    final Registration registration = get();
    if (registration != null && registration.isFromSettings())
      throw new IllegalStateException("The registration is configured through the settings '"
          + GlobalConfiguration.SUPPORT_CLIENT_ID.getKey() + "' and '" + GlobalConfiguration.SUPPORT_CLIENT_KEY.getKey()
          + "': remove them from the server configuration to unregister");
    if (Files.deleteIfExists(configDirectory.resolve(FILE_NAME)))
      LogManager.instance().log(this, Level.INFO, "Support registration removed");
  }

  public static void validateClientId(final String clientId) {
    if (clientId == null || clientId.isBlank())
      throw new IllegalArgumentException("The Client ID is required");
    final String id = clientId.trim();
    if (id.length() > MAX_CLIENT_ID_LENGTH || !id.chars().allMatch(c -> c > 32 && c < 127))
      throw new IllegalArgumentException("The Client ID is not valid: copy it from the customer portal");
  }

  public static void validateKey(final String key) {
    // The message never carries the key
    if (key == null || key.isBlank())
      throw new IllegalArgumentException("The Client key is required");
    final String k = key.trim();
    if (k.length() < MIN_KEY_LENGTH || k.length() > MAX_KEY_LENGTH || !k.chars().allMatch(c -> c > 32 && c < 127))
      throw new IllegalArgumentException("The Client key is not valid: copy it from the customer portal");
  }

  /**
   * HTTPS is required; plain HTTP is accepted only for localhost/127.0.0.1 (tests).
   *
   * @throws IllegalArgumentException with a message that never carries user info or a query of the URL
   */
  public static void validatePortalUrl(final String url) {
    final URI uri;
    try {
      uri = URI.create(url);
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException("The portal URL is not valid");
    }
    final String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
    final String host = uri.getHost();
    if (host == null || uri.getUserInfo() != null)
      throw new IllegalArgumentException("The portal URL is not valid");
    if (scheme.equals("https"))
      return;
    if (scheme.equals("http") && (host.equals("localhost") || host.equals("127.0.0.1") || host.equals("[::1]")))
      return;
    throw new IllegalArgumentException("The portal URL must use HTTPS (plain HTTP is accepted only for localhost)");
  }

  private JSONObject readFile() {
    final Path file = configDirectory.resolve(FILE_NAME);
    if (!Files.isRegularFile(file))
      return new JSONObject();
    try {
      return new JSONObject(Files.readString(file, StandardCharsets.UTF_8));
    } catch (final IOException | RuntimeException e) {
      // The message of a parse error can quote the content: it is not logged
      LogManager.instance().log(this, Level.WARNING, "Cannot read the support registration file '%s' (%s): ignored", file,
          e.getClass().getSimpleName());
      return new JSONObject();
    }
  }

  private static boolean firstExistingParentWritable(final Path dir) {
    Path p = dir.toAbsolutePath();
    while (p != null && !Files.exists(p))
      p = p.getParent();
    return p != null && Files.isWritable(p);
  }

  private static String trim(final String value) {
    return value == null ? "" : value.trim();
  }

  private static String stripTrailingSlash(final String url) {
    String u = url;
    while (u.endsWith("/"))
      u = u.substring(0, u.length() - 1);
    return u;
  }
}
