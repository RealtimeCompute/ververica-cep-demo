/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cep.demo.dynamic;

import com.alibaba.ververica.cep.demo.event.Event;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;

/** TestPatternProcessorDiscovererFactory. */
public class TestPatternProcessorDiscovererFactory
        implements PatternProcessorDiscovererFactory<Event> {
    private static final long serialVersionUID = 1L;
    private final String patternStr;

    public TestPatternProcessorDiscovererFactory(String patternStr) {
        this.patternStr = patternStr;
    }


    @Override
    public PatternProcessorDiscoverer<Event> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) {
        return new TestPatternProcessorDiscoverer(this.patternStr, userCodeClassLoader);
    }
}
