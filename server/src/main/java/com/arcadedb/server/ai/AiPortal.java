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
package com.arcadedb.server.ai;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.support.SupportConfiguration;
import com.arcadedb.server.support.SupportPortalClient;
import com.arcadedb.server.support.SupportPortalException;
import com.arcadedb.server.support.SupportService;

import java.util.logging.Level;

/**
 * Where the AI Assistant of this server gets its answers: the customer portal this server is connected to (the Client key it
 * already holds for support), or, for a server activated with a key of the old AI gateway, that gateway.
 * <p>
 * The portal wins whenever the server is connected and the portal says the plan includes the AI Assistant
 * ({@code ai-usage.enabled}); a connected server whose plan does not include it falls back to a legacy gateway key when it has
 * one, and otherwise is reported as not enabled with the reason, so Studio can offer the upgrade. The answer of
 * {@code ai-usage} is cached for a short time: Studio asks the status whenever the page opens.
 */
public class AiPortal {
  public enum Source {PORTAL, GATEWAY, NONE}

  /** How long the status of the plan is reused. Mutable and package-private only so a test can shorten it. */
  static volatile long usageTtlMs = 60_000L;
  /** How long a failure to read the status is remembered, so a portal that is down is not asked on every request. */
  static volatile long failureTtlMs = 10_000L;

  private final ArcadeDBServer  server;
  private final AiConfiguration config;

  private volatile JSONObject usage;
  private volatile String     failureCode;
  private volatile String     failureMessage;
  private volatile long       cachedAt;
  private volatile String     cachedFor = "";

  public AiPortal(final ArcadeDBServer server, final AiConfiguration config) {
    this.server = server;
    this.config = config;
  }

  private SupportConfiguration.Registration registration() {
    final SupportService support = server.getSupportService();
    return support == null ? null : support.getConfiguration().get();
  }

  /** Whether this server holds a portal registration (the Client key of the support connection). */
  public boolean isConnected() {
    return registration() != null;
  }

  /** The client of the portal this server is connected to, or null when it is not. */
  public AiPortalClient client() {
    final SupportConfiguration.Registration registration = registration();
    return registration == null ? null : new AiPortalClient(new SupportPortalClient(registration, server.getInstanceId()));
  }

  /** The portal page where the plan can be upgraded, or null when the server is not connected. */
  public String upgradeUrl() {
    final SupportConfiguration.Registration registration = registration();
    return registration == null ? null : registration.getPortalUrl() + "/#/subscription";
  }

  /** The source of the answers right now. */
  public Source source() {
    if (refresh() && usage.getBoolean("enabled", false))
      return Source.PORTAL;
    return config.isConfigured() ? Source.GATEWAY : Source.NONE;
  }

  /** Forgets the cached status of the plan (after a connection or a refusal that says it changed). */
  public void invalidate() {
    cachedAt = 0L;
  }

  /**
   * What Studio needs to draw the page: {@code {connected, enabled, portalUrl?, upgradeUrl?, tier?, turns?, spent?, budget?, percent?, resetsOn?,
   * code?, message?}}. Never carries the key.
   */
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    final SupportConfiguration.Registration registration = registration();
    json.put("connected", registration != null);
    json.put("enabled", false);
    if (registration == null)
      return json;
    json.put("portalUrl", registration.getPortalUrl());
    json.put("upgradeUrl", registration.getPortalUrl() + "/#/subscription");
    if (refresh()) {
      for (final String key : new String[] { "tier", "turns", "spent", "budget", "percent", "resetsOn" })
        if (usage.has(key))
          json.put(key, usage.get(key));
      json.put("enabled", usage.getBoolean("enabled", false));
    } else {
      json.put("code", failureCode);
      json.put("message", failureMessage);
    }
    return json;
  }

  /** Reads the status of the plan unless a recent answer is cached. @return true when {@link #usage} is valid */
  private synchronized boolean refresh() {
    final SupportConfiguration.Registration registration = registration();
    if (registration == null) {
      usage = null;
      return false;
    }
    // A different workspace or portal must never reuse another one's answer
    final String identity = registration.getPortalUrl() + "|" + registration.getClientId() + "|" + registration.getKeyHint();
    final long ttl = usage != null ? usageTtlMs : failureTtlMs;
    if (identity.equals(cachedFor) && System.currentTimeMillis() - cachedAt < ttl)
      return usage != null;

    cachedFor = identity;
    cachedAt = System.currentTimeMillis();
    try {
      usage = new AiPortalClient(new SupportPortalClient(registration, server.getInstanceId())).usage();
      failureCode = null;
      failureMessage = null;
      return true;
    } catch (final SupportPortalException e) {
      usage = null;
      failureCode = e.getCode();
      failureMessage = e.getMessage();
      LogManager.instance().log(this, Level.FINE, "AI Assistant: cannot read the plan from the portal (%s)", e.getCode());
      return false;
    } catch (final RuntimeException e) {
      usage = null;
      failureCode = "portal_error";
      failureMessage = "The portal answered in a form this server does not understand";
      return false;
    }
  }
}
