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

package org.apache.oozie.jobs.api.oozie.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Decision extends NodeBase {
    private final List<DagNodeWithCondition> childrenWithConditions;

    Decision(final String name) {
        super(name);
        this.childrenWithConditions = new ArrayList<>();
    }

    @Override
    public List<NodeBase> getChildren() {
        final List<NodeBase> results = new ArrayList<>();

        for (DagNodeWithCondition nodeWithCondition : childrenWithConditions) {
            results.add(nodeWithCondition.getNode());
        }

        return Collections.unmodifiableList(results);
    }

    public List<DagNodeWithCondition> getChildrenWithConditions() {
        return Collections.unmodifiableList(new ArrayList<>(childrenWithConditions));
    }

    @Override
    protected void addChild(NodeBase child) {
        throw new IllegalStateException("Decision nodes cannot have normal children.");
    }

    protected void addChildWithCondition(final NodeBase child, final String condition) {
        this.childrenWithConditions.add(new DagNodeWithCondition(child, condition));
    }

    @Override
    protected void removeChild(final NodeBase child) {
        int index = indexOfNodeBaseInChildrenWithConditions(childrenWithConditions, child);

        if (index < 0) {
            throw new IllegalArgumentException("Trying to remove a nonexistent child.");
        }

        this.childrenWithConditions.remove(index);
    }
}
