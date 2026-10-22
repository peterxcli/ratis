/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.netty.server;

import org.apache.ratis.protocol.ClientInvocationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Map: {@link ClientInvocationId} -> {@link STREAM}.
 *
 * @param <STREAM> the stream type.
 */
class StreamMap<STREAM> {
  public static final Logger LOG = LoggerFactory.getLogger(StreamMap.class);

  private final ConcurrentMap<ClientInvocationId, STREAM> map = new ConcurrentHashMap<>();

  STREAM computeIfAbsent(ClientInvocationId key, Function<ClientInvocationId, STREAM> function) {
    final STREAM info = map.computeIfAbsent(key, function);
    LOG.debug("computeIfAbsent({}) returns {}", key, info);
    return info;
  }

  STREAM remove(ClientInvocationId key) {
    final STREAM info = map.remove(key);
    LOG.debug("remove({}) returns {}", key, info);
    return info;
  }

  STREAM computeIfPresent(ClientInvocationId key, UnaryOperator<STREAM> function) {
    final STREAM info = map.computeIfPresent(key, (k, v) -> function.apply(v));
    LOG.debug("computeIfPresent({}) returns {}", key, info);
    return info;
  }

  /** Remove the streams matching the given predicate. */
  Map<ClientInvocationId, STREAM> removeIf(Predicate<STREAM> predicate) {
    final Map<ClientInvocationId, STREAM> removed = new HashMap<>();
    for (ClientInvocationId key : map.keySet()) {
      map.computeIfPresent(key, (k, v) -> {
        if (!predicate.test(v)) {
          return v;
        }
        removed.put(k, v);
        return null;
      });
    }
    LOG.debug("removeIf returns {}", removed.keySet());
    return removed;
  }
}
