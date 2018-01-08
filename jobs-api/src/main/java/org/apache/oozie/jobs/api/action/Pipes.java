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

package org.apache.oozie.jobs.api.action;

public class Pipes {
    private final String map;
    private final String reduce;
    private final String inputformat;
    private final String partitioner;
    private final String writer;
    private final String program;

    Pipes(final String map,
          final String reduce,
          final String inputformat,
          final String partitioner,
          final String writer,
          final String program) {
        this.map = map;
        this.reduce = reduce;
        this.inputformat = inputformat;
        this.partitioner = partitioner;
        this.writer = writer;
        this.program = program;
    }

    public String getMap() {
        return map;
    }

    public String getReduce() {
        return reduce;
    }

    public String getInputformat() {
        return inputformat;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public String getWriter() {
        return writer;
    }

    public String getProgram() {
        return program;
    }
}
