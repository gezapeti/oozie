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
import org.apache.oozie.jobs.api.ModifyOnce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Fork extends NodeBase {
    private NodeBase parent;
    private final List<NodeBase> children;

    private final ModifyOnce<Join> closingJoin;

    public Fork(final String name) {
        super(name);

        this.parent = null;
        this.children = new ArrayList<>();
        this.closingJoin = new ModifyOnce<>();
    }

    public NodeBase getParent() {
        return parent;
    }

    @Override
    public void addParent(final NodeBase parent) {
        Preconditions.checkState(this.parent == null, "Fork nodes cannot have multiple parents.");

        this.parent = parent;
        parent.addChild(this);
    }

    @Override
    public void addParentWithCondition(final Decision parent, final Condition condition) {
        Preconditions.checkState(this.parent == null, "Fork nodes cannot have multiple parents.");

        this.parent = parent;
        parent.addChildWithCondition(this, condition);
    }

    @Override
    public void addParentDefaultConditional(Decision parent) {
        Preconditions.checkState(this.parent == null, "Fork nodes cannot have multiple parents.");

        this.parent = parent;
        parent.addDefaultChild(this);
    }

    @Override
    public void removeParent(final NodeBase parent) {
        Preconditions.checkArgument(this.parent == parent, "Trying to remove a nonexistent parent.");

        if (this.parent != null) {
            this.parent.removeChild(this);
        }

        this.parent = null;
    }

    @Override
    public void clearParents() {
        removeParent(parent);
    }

    @Override
    public List<NodeBase> getChildren() {
        return Collections.unmodifiableList(new ArrayList<>(children));
    }

    Join getClosingJoin() {
        return closingJoin.get();
    }

    boolean isClosed() {
        return getClosingJoin() != null;
    }

    /* package private */ void close(final Join join) {
        closingJoin.set(join);
    }

    @Override
    protected void addChild(final NodeBase child) {
        children.add(child);
    }

    @Override
    protected void removeChild(final NodeBase child) {
        Preconditions.checkArgument(this.children.remove(child),"Trying to remove a nonexistent child.");
    }
}
