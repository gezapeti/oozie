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

import com.google.common.base.Preconditions;
import org.apache.oozie.jobs.api.Condition;

import java.util.Arrays;
import java.util.List;

public class Start extends NodeBase {
    private NodeBase child;

    public Start(final String name) {
        super(name);
    }

    public NodeBase getChild() {
        return child;
    }

    @Override
    public void addParent(final NodeBase parent) {
        throw new IllegalStateException("Start nodes cannot have parents.");
    }

    @Override
    public void addParentWithCondition(final Decision parent, final Condition condition) {
        throw new IllegalStateException("Start nodes cannot have parents.");
    }

    @Override
    public void addParentDefaultConditional(Decision parent) {
        throw new IllegalStateException("Start nodes cannot have parents.");
    }

    @Override
    public void removeParent(NodeBase parent) {
        throw new IllegalStateException("Start nodes cannot have parents.");
    }

    @Override
    public void clearParents() {

    }

    @Override
    public List<NodeBase> getChildren() {
        if (child == null) {
            return Arrays.asList();
        }

        return Arrays.asList(child);
    }

    @Override
    protected void addChild(final NodeBase child) {
        Preconditions.checkState(this.child == null, "Start nodes cannot have multiple children.");

        this.child =  child;
    }

    @Override
    protected void removeChild(final NodeBase child) {
        Preconditions.checkArgument(this.child == child, "Trying to remove a nonexistent child.");

        this.child = null;
    }
}
