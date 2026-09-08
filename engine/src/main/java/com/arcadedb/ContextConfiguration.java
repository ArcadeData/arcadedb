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
package com.arcadedb;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.SystemVariableResolver;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Represents a context configuration where custom setting could be defined for the context only. If not defined, globals will be
 * taken.
 **/
public class ContextConfiguration implements Serializable {
  private final           Map<String, Object>    config         = new ConcurrentHashMap<String, Object>();
  /**
   * Keys already reported by {@link #getValueAsBoolean(GlobalConfiguration)} as holding something that is not
   * boolean text. Per-instance rather than static so one test's bad value cannot mute another's report.
   */
  private final           Set<String>            nonBooleanReported = ConcurrentHashMap.newKeySet();
  private transient final SystemVariableResolver customResolver = new SystemVariableResolver() {
    @Override
    public String resolve(final String variable) {
      Object result = config.get(variable);
      if (result == null)
        result = super.resolve(variable);
      return result != null ? result.toString() : null;
    }
  };

  /**
   * Empty constructor to create just a proxy for the GlobalConfiguration. No values are set.
   */
  public ContextConfiguration() {
  }

  /**
   * Initializes the context with custom parameters.
   *
   * @param iConfig Map of parameters of type {@literal Map<String, Object>}.
   */
  public ContextConfiguration(final Map<String, Object> iConfig) {
    this.config.putAll(iConfig);
  }

  public ContextConfiguration(final ContextConfiguration iParent) {
    if (iParent != null)
      config.putAll(iParent.config);
  }

  /**
   * Loads the server configuration file into this overlay, applying each value's declared type on the way in.
   * <p>
   * Issue #7262. This used to store the raw JSON value with a plain {@code put}, which for a {@code Boolean}
   * setting meant the string survived to the read site and was then read by {@code Boolean.parseBoolean} - so
   * {@code "arcadedb.ha.tls.mutualAuth": "yes"} silently DISABLED mutual TLS authentication on the Raft channel,
   * and {@code "arcadedb.ha.peerAllowlist.enabled": "on"} silently skipped installing the peer allowlist. Both
   * default to {@code true}, so the operator who never mentioned them was safe and the one who wrote down that
   * they wanted the protection was the one who lost it - the exact opposite of the fail-safe property #7222
   * established for the system-property and environment-variable path.
   * <p>
   * A value the setting's type cannot read is REFUSED and the setting keeps its default, reported once through
   * {@link GlobalConfiguration#coerceFromConfigurationSource(Object, String)} - the same parse and the same
   * message that path uses. Refusing rather than guessing is what makes this fail closed for a switch whose
   * default is the protection: nothing about a typo says which way its author meant it.
   * <p>
   * Note that a REFUSED value is not stored, so {@link #toJSON()} no longer round-trips it. That is deliberate:
   * this map is what a server hands to its plugins, and a value nothing can read has no business being in it.
   */
  public void fromJSON(final String input) {
    if (input == null)
      return;

    final JSONObject json = new JSONObject(input);

    final JSONObject cfg = json.getJSONObject("configuration");
    for (final String k : cfg.keySet()) {
      final GlobalConfiguration cfgEntry = GlobalConfiguration.findByKey(GlobalConfiguration.PREFIX + k);
      if (cfgEntry != null) {
        final Object value = cfg.get(k);
        final Object coerced = cfgEntry.coerceFromConfigurationSource(value, "server configuration file");
        if (coerced == null)
          // EITHER REFUSED (ALREADY REPORTED) OR A JSON null. LEAVE THE SETTING ON ITS DEFAULT RATHER THAN GUESS
          // WHAT WAS MEANT - AND NOTE THE MAP IS A ConcurrentHashMap, SO STORING null WOULD THROW ANYWAY.
          continue;

        config.put(GlobalConfiguration.PREFIX + k, coerced);
        cfgEntry.applyContextValue(coerced);
      }
    }
  }

  public String toJSON() {
    final JSONObject json = new JSONObject();

    final JSONObject cfg = new JSONObject();
    json.put("configuration", cfg);

    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      cfg.put(entry.getKey().substring(GlobalConfiguration.PREFIX.length()), entry.getValue());
    }

    return json.toString();
  }

  public Object setValue(final GlobalConfiguration iConfig, final Object iValue) {
    final Object previous = iValue == null ? config.remove(iConfig.getKey()) : config.put(iConfig.getKey(), iValue);
    iConfig.applyContextValue(iValue);
    return previous;
  }

  public Object setValue(final String iName, final Object iValue) {
    final Object previous = iValue == null ? config.remove(iName) : config.put(iName, iValue);
    applyDeclaredSideEffect(iName, iValue);
    return previous;
  }

  /**
   * Runs the side effect of a declared SCOPE.SERVER setting written into this overlay.
   * <p>
   * This map is where a server's settings actually live: {@link #fromJSON} loads the server configuration file into
   * it, and both {@code SET SERVER SETTING} and the MCP {@code set_server_setting} tool write through
   * {@link #setValue}. None of them touches the {@link GlobalConfiguration} enum, so a setting whose effect is a
   * side effect rather than a value someone later reads used to be stored here and never applied - which is what
   * made {@code arcadedb.server.logFormat} work only as a raw {@code -D} (issue #7121). Firing it here rather than
   * at each caller means the next SCOPE.SERVER setting that needs a side effect gets it on every channel by
   * declaring a callback, instead of relying on someone remembering all three call sites.
   */
  private void applyDeclaredSideEffect(final String key, final Object value) {
    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(key);
    if (cfg != null)
      cfg.applyContextValue(value);
  }

  public <T> T getValue(final GlobalConfiguration iConfig) {
    if (config.containsKey(iConfig.getKey()))
      return (T) config.get(iConfig.getKey());
    return iConfig.getValue();
  }

  /**
   * @param config Global configuration parameter.
   *
   * @return Value of configuration parameter stored in this context as enumeration if such one exists, otherwise value stored in
   * passed in {@link GlobalConfiguration} instance.
   *
   * @throws ClassCastException       if stored value can not be casted and parsed from string to passed in enumeration class.
   * @throws IllegalArgumentException if value associated with configuration parameter is a string bug can not be converted to
   *                                  instance of passed in enumeration class.
   */
  public <T extends Enum<T>> T getValueAsEnum(final GlobalConfiguration config, final Class<T> enumType) {
    final Object value;
    if (this.config.containsKey(config.getKey())) {
      value = this.config.get(config.getKey());
    } else {
      value = config.getValue();
    }

    if (value == null)
      return null;

    if (enumType.isAssignableFrom(value.getClass())) {
      return enumType.cast(value);
    } else if (value instanceof String) {
      final String presentation = value.toString().toUpperCase(Locale.ENGLISH);
      return Enum.valueOf(enumType, presentation);
    } else {
      throw new ClassCastException("Value " + value + " can not be cast to enumeration " + enumType.getSimpleName());
    }
  }

  public boolean hasValue(final String iName) {
    return config.containsKey(iName);
  }

  @SuppressWarnings("unchecked")
  public <T> T getValue(final String iName, final T defaultValue) {
    if (config.containsKey(iName))
      return (T) config.get(iName);

    final String sysProperty = System.getProperty(iName);
    if (sysProperty != null)
      return (T) sysProperty;

    return defaultValue;
  }

  /**
   * Issue #7262: the second half of the mechanism that let {@code "arcadedb.ha.tls.mutualAuth": "yes"} disable
   * mutual TLS on the Raft channel. {@code Boolean.parseBoolean} maps every string that is not {@code "true"} to
   * {@code false}, so a synonym, a typo or a stray space read as a deliberate opt-OUT - and for the several
   * settings whose default is {@code true} because the default is the protection, that is the one direction a
   * misread must never take.
   * <p>
   * {@link #fromJSON(String)} now refuses such a value on the way in, which is where the operator can be told
   * about it. This is the backstop for the entry points that still hand over untyped values - the
   * {@link #ContextConfiguration(Map) map constructor}, {@link #merge(ContextConfiguration)}, and
   * {@link #setValue(String, Object)} called with raw text - and it answers the same way that path does: a value
   * that is not boolean text is REFUSED, reported once, and the value the setting would have had without it is
   * returned. Falling back to the default rather than to {@code false} is what makes this fail closed for a default-{@code true} switch
   * without flipping the settings whose safe side is the other one; refusing to guess is the property that holds
   * for all of them.
   *
   * @return the configured value, or {@code iConfig}'s default when the configured one is not boolean text
   */
  public boolean getValueAsBoolean(final GlobalConfiguration iConfig) {
    // One map lookup, reused by the refusal branch below to tell an overlay value from the enum's own.
    final boolean fromOverlay = config.containsKey(iConfig.getKey());
    final Object v = fromOverlay ? config.get(iConfig.getKey()) : iConfig.getValue();
    if (v == null)
      return false;
    if (v instanceof Boolean b)
      return b;

    final String text = v.toString().trim();
    if ("true".equalsIgnoreCase(text))
      return true;
    if ("false".equalsIgnoreCase(text))
      return false;

    reportNonBoolean(iConfig, v);

    // "As if this key had never been written": a bad value in THIS overlay falls back to the setting's
    // process-wide value, which is its compiled-in default unless a system property or an environment variable
    // chose one - and a bad value already on the enum falls back to the compiled-in default itself. Same rule
    // GlobalConfiguration.setValueFromConfigurationSource applies to a refusal on the -D path.
    final Object fallback = fromOverlay ? iConfig.getValue() : iConfig.getDefValue();
    return fallback instanceof Boolean b && b;
  }

  /**
   * Reports a value that is not boolean text ONCE per setting. The read sites are on connection and request paths,
   * so logging on every read would turn one mistyped setting into a flood; the message is about a configuration
   * mistake that does not change while the process runs, so the first one says everything the operator needs.
   */
  private void reportNonBoolean(final GlobalConfiguration iConfig, final Object value) {
    if (!nonBooleanReported.add(iConfig.getKey()))
      return;

    LogManager.instance().log(this, Level.WARNING,
        "Invalid value %s for setting '%s': only 'true' and 'false' are accepted. Keeping the default '%s'",
        iConfig.redactIfHidden(value), iConfig.getKey(), iConfig.getDefValue());
  }

  /**
   * Returns {@code true} when HA is implicitly opted-in by a non-blank {@code HA_SERVER_LIST}.
   * Combined with an explicit {@code HA_ENABLED=true}, this determines whether the Raft plugin
   * is discovered and started. Kept here as the single source of truth so the {@code server}
   * and {@code ha-raft} modules stay in sync.
   */
  public boolean isHAImplicitlyEnabled() {
    final String serverList = getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    return serverList != null && !serverList.isBlank();
  }

  public String getValueAsString(final String iName, final String iDefaultValue) {
    return getValue(iName, iDefaultValue);
  }

  public String getValueAsString(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return null;

    return getVariable(v.toString(), "");
  }

  /**
   * Issue #6875: this reads through {@link FileUtils#getSizeAsNumber(Object)}, not {@code Integer.parseInt}, so that
   * it and {@link GlobalConfiguration#getValueAsInteger()} are one parse rather than two that disagree. The context
   * map can hold a raw string that never passed through {@link GlobalConfiguration#coerce(Object)} - {@link #fromJSON}
   * and the {@link Map} constructor both put one straight in - and such a value used to read as 1048576 through the
   * global accessor and throw {@code NumberFormatException} through this one. {@code getSizeAsNumber} is a strict
   * superset of {@code Integer.parseInt}, so nothing that read before stops reading.
   */
  public int getValueAsInteger(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return iConfig.narrowToInteger(v instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(v.toString().trim()));
  }

  public long getValueAsLong(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(v.toString().trim());
  }

  public float getValueAsFloat(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Number n ? n.floatValue() : Float.parseFloat(v.toString().trim());
  }

  public Set<String> getContextKeys() {
    return config.keySet();
  }

  public void merge(final ContextConfiguration contextConfiguration) {
    this.config.putAll(contextConfiguration.config);
  }

  public void reset() {
    config.clear();
  }

  private String getVariable(final String name, final String defValue) {
    String result = customResolver.resolveSystemVariables(name, defValue);
    if (result == null)
      result = defValue;
    return result;
  }
}
