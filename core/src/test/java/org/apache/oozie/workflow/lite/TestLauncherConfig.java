/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.workflow.lite;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public class TestLauncherConfig {

    @Test
    public void testSerializeAndDeserializeDefaultLauncherConfig() throws IOException {
        LauncherConfig config = LauncherConfig.DEFAULT_LAUNCHER_CONFIG;

        testSerializationAndDeserialization(config);
    }

    @Test
    public void testSerializeAndDeserializeLauncherConfig() throws IOException {
        Map<String, String> env = new HashMap<>();
        env.put("key1", "val1");
        env.put("key2", "val2");
        List<String> sharelibs = new ArrayList<>();
        sharelibs.add("lib1");
        sharelibs.add("lib2");

        LauncherConfig config = new LauncherConfig(2,
                2048,
                env,
                "-Dtest",
                "queue",
                sharelibs);
 
        testSerializationAndDeserialization(config);
    }

    private void testSerializationAndDeserialization(LauncherConfig config) throws IOException {
        ByteArrayDataOutput output = ByteStreams.newDataOutput();

        config.write(output);
        ByteArrayDataInput input = ByteStreams.newDataInput(output.toByteArray());
        LauncherConfig deserializedConfig = LauncherConfig.fromWritable(input);

        assertEquals("Config", config, deserializedConfig);
    }
}