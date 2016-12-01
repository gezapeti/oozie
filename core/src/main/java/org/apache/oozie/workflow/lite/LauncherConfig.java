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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.util.WritableUtils;

public class LauncherConfig implements Writable {
    public static final int DEFAULT_VCORES = 1;
    public static final int DEFAULT_MEMORY = 1024;

    public static final LauncherConfig DEFAULT_LAUNCHER_CONFIG = 
            new LauncherConfig(LauncherConfig.DEFAULT_VCORES,
                    LauncherConfig.DEFAULT_MEMORY,
                    Collections.<String,String>emptyMap(),
                    null,
                    null,
                    Collections.<String>emptyList());

    private int vcores;
    private int memory;
    private Map<String, String> env;
    private String javaOpts;
    private String queue;
    private List<String> sharelibs;

    public LauncherConfig(int vcores, int memory, Map<String, String> env, String javaOpts, String queue, List<String> sharelibs) {
        this.vcores = vcores;
        this.memory = memory;
        this.env = Collections.unmodifiableMap(new HashMap<>(env));
        this.javaOpts = javaOpts;
        this.queue = queue;
        this.sharelibs = Collections.unmodifiableList(new ArrayList<>(sharelibs));
    }

    private LauncherConfig() {
        // when constructed from Writable
    }

    public int getVcores() {
        return vcores;
    }

    public int getMemory() {
        return memory;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public String getJavaOpts() {
        return javaOpts;
    }


    public String getQueue() {
        return queue;
    }

    public List<String> getSharelibs() {
        return sharelibs;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(memory);
        out.writeInt(vcores);
        WritableUtils.writeStr(out, javaOpts);
        WritableUtils.writeStr(out, queue);
        WritableUtils.writeStringMap(out, env);
        WritableUtils.writeStringList(out, sharelibs);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        memory = in.readInt();
        vcores = in.readInt();
        javaOpts = WritableUtils.readStr(in);
        queue = WritableUtils.readStr(in);
        env = WritableUtils.readStringMap(in);
        sharelibs = WritableUtils.readStringList(in);
    }

    public static LauncherConfig fromWritable(DataInput in) throws IOException {
        LauncherConfig config = new LauncherConfig();
        config.readFields(in);
        return config;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((env == null) ? 0 : env.hashCode());
        result = prime * result + ((javaOpts == null) ? 0 : javaOpts.hashCode());
        result = prime * result + memory;
        result = prime * result + ((queue == null) ? 0 : queue.hashCode());
        result = prime * result + ((sharelibs == null) ? 0 : sharelibs.hashCode());
        result = prime * result + vcores;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LauncherConfig other = (LauncherConfig) obj;
        if (env == null) {
            if (other.env != null)
                return false;
        } else if (!env.equals(other.env))
            return false;
        if (javaOpts == null) {
            if (other.javaOpts != null)
                return false;
        } else if (!javaOpts.equals(other.javaOpts))
            return false;
        if (memory != other.memory)
            return false;
        if (queue == null) {
            if (other.queue != null)
                return false;
        } else if (!queue.equals(other.queue))
            return false;
        if (sharelibs == null) {
            if (other.sharelibs != null)
                return false;
        } else if (!sharelibs.equals(other.sharelibs))
            return false;
        if (vcores != other.vcores)
            return false;
        return true;
    }
}