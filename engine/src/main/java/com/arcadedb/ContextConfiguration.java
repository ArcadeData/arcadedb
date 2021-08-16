/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb;

import com.arcadedb.utility.SystemVariableResolver;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a context configuration where custom setting could be defined for the context only. If not defined, globals will be
 * taken.
 **/
public class ContextConfiguration implements Serializable {
  private final Map<String, Object> config = new ConcurrentHashMap<String, Object>();

  /**
   * Empty constructor to create just a proxy for the OGlobalConfiguration. No values are setted.
   */
  public ContextConfiguration() {
  }

  /**
   * Initializes the context with custom parameters.
   *
   * @param iConfig Map of parameters of type Map<String, Object>.
   */
  public ContextConfiguration(final Map<String, Object> iConfig) {
    this.config.putAll(iConfig);
  }

  public ContextConfiguration(final ContextConfiguration iParent) {
    if (iParent != null)
      config.putAll(iParent.config);
  }

  public void fromJSON(final String input) {
    if (input == null)
      return;

    final JSONObject json = new JSONObject(input);

    final JSONObject cfg = json.getJSONObject("configuration");
    for (String k : cfg.keySet()) {
      final GlobalConfiguration cfgEntry = GlobalConfiguration.findByKey(GlobalConfiguration.PREFIX + k);
      if (cfgEntry != null) {
        config.put(GlobalConfiguration.PREFIX + k, cfg.get(k));
      }
    }
  }

  public String toJSON() {
    final JSONObject json = new JSONObject();

    final JSONObject cfg = new JSONObject();
    json.put("configuration", cfg);

    for (String k : config.keySet()) {
      cfg.put(k.substring(GlobalConfiguration.PREFIX.length()), config.get(k));
    }

    return json.toString();
  }

  public Object setValue(final GlobalConfiguration iConfig, final Object iValue) {
    if (iValue == null)
      return config.remove(iConfig.getKey());

    return config.put(iConfig.getKey(), iValue);
  }

  public Object setValue(final String iName, final Object iValue) {
    if (iValue == null)
      return config.remove(iName);

    return config.put(iName, iValue);
  }

  public Object getValue(final GlobalConfiguration iConfig) {
    if (config != null && config.containsKey(iConfig.getKey()))
      return config.get(iConfig.getKey());
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
  public <T extends Enum<T>> T getValueAsEnum(final GlobalConfiguration config, Class<T> enumType) {
    final Object value;
    if (this.config != null && this.config.containsKey(config.getKey())) {
      value = this.config.get(config.getKey());
    } else {
      value = config.getValue();
    }

    if (value == null)
      return null;

    if (enumType.isAssignableFrom(value.getClass())) {
      return enumType.cast(value);
    } else if (value instanceof String) {
      final String presentation = value.toString();
      return Enum.valueOf(enumType, presentation);
    } else {
      throw new ClassCastException("Value " + value + " can not be cast to enumeration " + enumType.getSimpleName());
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getValue(final String iName, final T iDefaultValue) {
    if (config != null && config.containsKey(iName))
      return (T) config.get(iName);

    final String sysProperty = System.getProperty(iName);
    if (sysProperty != null)
      return (T) sysProperty;

    return iDefaultValue;
  }

  public boolean getValueAsBoolean(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return false;
    return v instanceof Boolean ? ((Boolean) v).booleanValue() : Boolean.parseBoolean(v.toString());
  }

  public String getValueAsString(final String iName, final String iDefaultValue) {
    return getValue(iName, iDefaultValue);
  }

  public String getValueAsString(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return null;

    return SystemVariableResolver.resolveSystemVariables(v.toString(), "");
  }

  public int getValueAsInteger(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Integer ? ((Integer) v).intValue() : Integer.parseInt(v.toString());
  }

  public long getValueAsLong(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Long ? ((Long) v).longValue() : Long.parseLong(v.toString());
  }

  public float getValueAsFloat(final GlobalConfiguration iConfig) {
    final Object v = getValue(iConfig);
    if (v == null)
      return 0;
    return v instanceof Float ? ((Float) v).floatValue() : Float.parseFloat(v.toString());
  }

  public int getContextSize() {
    return config.size();
  }

  public java.util.Set<String> getContextKeys() {
    return config.keySet();
  }

  public void merge(ContextConfiguration contextConfiguration) {
    this.config.putAll(contextConfiguration.config);
  }

  public void reset() {
    config.clear();
  }
}
