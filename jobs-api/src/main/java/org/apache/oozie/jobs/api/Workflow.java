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

package org.apache.oozie.jobs.api;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Workflow {
    private final String name;
    private final ImmutableSet<Node> nodes;
    private final ImmutableSet<Node> roots;

    Workflow(String name, ImmutableSet<Node> nodes) {
        this.name = name;
        this.nodes = nodes;
        this.roots = filterRoots(nodes);
    }

    public String getName() {
        return name;
    }

    public ImmutableSet<Node> getNodes() {
        return nodes;
    }

    public ImmutableSet<Node> getRoots() {
        return roots;
    }

    private static ImmutableSet<Node> filterRoots(Set<Node> dag) {
        ImmutableSet.Builder<Node> builder = new ImmutableSet.Builder<>();
        for (Node node : dag) {
            if (node.getParents().isEmpty()) {
                builder.add(node);
            }
        }

        return builder.build();
    }
}
