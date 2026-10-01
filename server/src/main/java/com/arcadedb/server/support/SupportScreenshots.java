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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Screenshots a user pasted (or dropped, or picked) in the Support page, held in memory until they are sent or expire. A
 * screenshot of a query result is the most common attachment of a support issue, so a user can add one to a new issue, a reply
 * or the files sent to an existing issue; the browser stages each picture here as soon as it is added, shows it back as a
 * thumbnail, and refers to it by id when the user clicks Send. NOTHING leaves the server until then, which keeps the rule of
 * the whole Support page: the user reviews exactly what is sent.
 *
 * <h2>What a screenshot is</h2>
 *
 * <p>A PNG, JPEG, GIF or WebP, recognised by its first bytes and by nothing else: the type a browser declares and the name it
 * gives are the sender's. SVG is not accepted: it is a document that can carry script. The bytes are never decoded here. The
 * same rules as the portal's platform (which checks again).
 *
 * <p>Bounded in what it holds: {@link #MAX_BYTES} a picture, {@link #MAX_HELD} pictures and {@link #MAX_TOTAL_BYTES} in all,
 * for {@link #TTL_MS}; expired ones are dropped on the next call.
 */
public class SupportScreenshots {
  public static final int  MAX_BYTES       = 5 * 1024 * 1024;
  public static final int  MAX_HELD        = 10;
  public static final long MAX_TOTAL_BYTES = 30L * 1024 * 1024;
  public static final int  MAX_PER_SEND    = 5;
  public static final long TTL_MS          = SupportBundleManager.TTL_MS;

  private static final SecureRandom RANDOM = new SecureRandom();

  /** One staged picture. */
  public record Shot(String id, String mediaType, String extension, byte[] bytes, long expiresAt) {
    public String filename() {
      return "screenshot." + extension;
    }
  }

  private final Map<String, Shot> held = new LinkedHashMap<>();

  /** The extension (png, jpg, gif or webp) of what these bytes are, or null when they are not a picture accepted here. */
  public static String extensionOf(final byte[] b) {
    if (b == null || b.length < 12)
      return null;
    if ((b[0] & 0xFF) == 0x89 && b[1] == 'P' && b[2] == 'N' && b[3] == 'G' && b[4] == 0x0D && b[5] == 0x0A && b[6] == 0x1A
        && b[7] == 0x0A)
      return "png";
    if ((b[0] & 0xFF) == 0xFF && (b[1] & 0xFF) == 0xD8 && (b[2] & 0xFF) == 0xFF)
      return "jpg";
    if (b[0] == 'G' && b[1] == 'I' && b[2] == 'F' && b[3] == '8' && (b[4] == '7' || b[4] == '9') && b[5] == 'a')
      return "gif";
    if (b[0] == 'R' && b[1] == 'I' && b[2] == 'F' && b[3] == 'F' && b[8] == 'W' && b[9] == 'E' && b[10] == 'B' && b[11] == 'P')
      return "webp";
    return null;
  }

  public static String mediaTypeOf(final String extension) {
    return switch (extension) {
      case "png" -> "image/png";
      case "jpg" -> "image/jpeg";
      case "gif" -> "image/gif";
      default -> "image/webp";
    };
  }

  /** Holds a picture and answers its id. */
  public synchronized Shot stage(final byte[] bytes) {
    dropExpired();
    if (bytes == null || bytes.length == 0)
      throw new SupportException("bad_request", "The screenshot is empty");
    if (bytes.length > MAX_BYTES)
      throw new SupportException("too_large", "A screenshot is at most " + MAX_BYTES / (1024 * 1024) + " MB");
    final String extension = extensionOf(bytes);
    if (extension == null)
      throw new SupportException("bad_request", "A screenshot must be a PNG, JPEG, GIF or WebP image");
    long total = bytes.length;
    for (final Shot shot : held.values())
      total += shot.bytes().length;
    if (held.size() >= MAX_HELD || total > MAX_TOTAL_BYTES)
      throw new SupportException("bad_request", "Too many screenshots are waiting to be sent: send or remove some first");
    final byte[] random = new byte[12];
    RANDOM.nextBytes(random);
    final Shot shot = new Shot("shot_" + HexFormat.of().formatHex(random), mediaTypeOf(extension), extension, bytes,
        System.currentTimeMillis() + TTL_MS);
    held.put(shot.id(), shot);
    return shot;
  }

  /**
   * The pictures named, in the order named, WITHOUT removing them (a send that fails must be retryable). At most
   * {@link #MAX_PER_SEND}; a repeated id counts once.
   */
  public synchronized List<Shot> peek(final List<String> ids) {
    dropExpired();
    final List<Shot> out = new ArrayList<>();
    for (final String id : ids) {
      final Shot shot = held.get(id);
      if (shot == null)
        throw new SupportException("screenshot_not_found", "A screenshot expired or was removed: add it again");
      if (!out.contains(shot))
        out.add(shot);
    }
    if (out.size() > MAX_PER_SEND)
      throw new SupportException("bad_request", "At most " + MAX_PER_SEND + " screenshots can be sent at a time");
    return out;
  }

  public synchronized void remove(final List<Shot> shots) {
    for (final Shot shot : shots)
      held.remove(shot.id());
  }

  /** Forgets one picture the user removed. Unknown ids are not an error: the browser and the server may disagree after an expiry. */
  public synchronized void discard(final String id) {
    held.remove(id);
  }

  public synchronized int size() {
    dropExpired();
    return held.size();
  }

  private void dropExpired() {
    final long now = System.currentTimeMillis();
    for (final Iterator<Shot> it = held.values().iterator(); it.hasNext(); )
      if (it.next().expiresAt() <= now)
        it.remove();
  }
}
