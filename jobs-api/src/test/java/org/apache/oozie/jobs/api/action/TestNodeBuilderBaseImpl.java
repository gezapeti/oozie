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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class TestNodeBuilderBaseImpl <N extends Node,
        B extends NodeBuilderBaseImpl<B> & Builder<N>> {
    static final String NAME = "map-reduce-name";

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    protected abstract B getBuilderInstance();
    protected abstract B getBuilderInstance(N action);

    @Test
    public final void testIncorrectSubclassingThrows() {
        class WrongBuilder extends NodeBuilderBaseImpl<MapReduceActionBuilder> implements Builder<MapReduceAction> {

            private WrongBuilder() {
                super();
            }

            public MapReduceActionBuilder getRuntimeSelfReference() {
                return new MapReduceActionBuilder();
            }

            @Override
            public MapReduceAction build() {
                return null;
            }
        }

        expectedException.expect(IllegalStateException.class);

        new WrongBuilder().withName("obsolete");
    }

    @Test
    public void testAddParents() {
        final N parent1 = Mockito.spy(getBuilderInstance().build());
        final N parent2 = Mockito.spy(getBuilderInstance().build());

        final B builder = getBuilderInstance();
        builder.withParent(parent1)
                .withParent(parent2);

        final N child = builder.build();

        assertEquals(Arrays.asList(parent1, parent2), child.getAllParents());

        Mockito.verify(parent1).addChild(child);
        Mockito.verify(parent2).addChild(child);

        Mockito.verifyNoMoreInteractions(parent1);
        Mockito.verifyNoMoreInteractions(parent2);
    }

    @Test
    public void testWithConditionalParents() {

    }

    @Test
    public void testAddingDuplicateParentBothTimesWithoutConditionThrows() {
        final N parent = getBuilderInstance().build();

        final B builder = getBuilderInstance();

        builder.withParent(parent);

        expectedException.expect(IllegalArgumentException.class);
        builder.withParent(parent);
    }

    @Test
    public void testAddingDuplicateParentBothTimesWithConditionThrows() {
        final N parent = getBuilderInstance().build();

        final B builder = getBuilderInstance();

        builder.withParentWithCondition(parent, "condition1");

        expectedException.expect(IllegalArgumentException.class);
        builder.withParentWithCondition(parent, "condition2");
    }

    @Test
    public void testAddingDuplicateParentFirstWithoutConditionThenWithConditionThrows() {
        final N parent = getBuilderInstance().build();

        final B builder = getBuilderInstance();

        builder.withParent(parent);

        expectedException.expect(IllegalArgumentException.class);
        builder.withParentWithCondition(parent, "any_condition");
    }

    @Test
    public void testAddingDuplicateParentFirstWithConditionThenWithoutConditionThrows() {
        final N parent = getBuilderInstance().build();

        final B builder = getBuilderInstance();

        builder.withParentWithCondition(parent, "condition");

        expectedException.expect(IllegalArgumentException.class);
        builder.withParent(parent);
    }

    @Test
    public void testWithoutParent() {
        final N parent1 = Mockito.spy(getBuilderInstance().build());
        final N parent2 = Mockito.spy(getBuilderInstance().build());

        final B builder = getBuilderInstance();
        builder.withParent(parent1)
                .withParent(parent2);

        builder.withoutParent(parent2);

        final N child = builder.build();

        assertEquals(Arrays.asList(parent1), child.getAllParents());

        Mockito.verify(parent1).addChild(child);

        Mockito.verifyNoMoreInteractions(parent1);
        Mockito.verifyNoMoreInteractions(parent2);
    }

    @Test
    public void testAddParentWithAndWithoutCondition() {
        final Node parent1 = getBuilderInstance().build();
        final Node parent2 = getBuilderInstance().build();

        final String condition = "condition";

        final Node child = getBuilderInstance()
                .withParent(parent1)
                .withParentWithCondition(parent2, condition)
                .build();

        assertEquals(Arrays.asList(parent1, parent2), child.getAllParents());
        assertEquals(Arrays.asList(parent1), child.getParentsWithoutConditions());

        final List<Node.NodeWithCondition> parentsWithConditions = child.getParentsWithConditions();
        assertEquals(parent2, parentsWithConditions.get(0).getNode());
        assertEquals(condition, parentsWithConditions.get(0).getCondition());
    }

    @Test
    public void testAddMultipleDefaultConditionalChildrenThrows() {
        final N parent = getBuilderInstance().withName("parent").build();

        final N defaultChild1 = getBuilderInstance().withName("defaultChild1").withParentDefaultConditional(parent).build();

        final B defaultChild2Builder = getBuilderInstance().withName("defaultChild2").withParentDefaultConditional(parent);

        expectedException.expect(IllegalStateException.class);
        defaultChild2Builder.build();
    }

    @Test
    public void testWithoutParentWhenConditionExists() {
        final Node parent1 = getBuilderInstance().build();
        final Node parent2 = getBuilderInstance().build();
        final Node parent3 = getBuilderInstance().build();
        final Node parent4 = getBuilderInstance().build();

        final String condition1 = "condition1";
        final String condition2 = "condition2";

        final B builder = getBuilderInstance()
                .withParentWithCondition(parent1, condition1)
                .withParentWithCondition(parent2, condition2)
                .withParent(parent3)
                .withParent(parent4);

        builder.withoutParent(parent2);

        final N child = builder.build();

        assertEquals(Arrays.asList(parent3, parent4, parent1), child.getAllParents());
        assertEquals(Arrays.asList(parent3, parent4), child.getParentsWithoutConditions());

        final List<Node.NodeWithCondition> parentsWithConditions = child.getParentsWithConditions();
        assertEquals(parent1, parentsWithConditions.get(0).getNode());
        assertEquals(condition1, parentsWithConditions.get(0).getCondition());
    }

    @Test
    public void testAddedAsParentWithCondition() {
        final N parent = getBuilderInstance().withName("parent").build();

        final String condition1 = "condition1";
        final String condition2 = "condition2";

        final N child1 = getBuilderInstance().withName("child1").withParentWithCondition(parent, condition1).build();
        final N child2 = getBuilderInstance().withName("child2").withParentWithCondition(parent, condition2).build();
        final N defaultChild = getBuilderInstance().withName("defaultChild").withParentDefaultConditional(parent).build();


        final List<Node.NodeWithCondition> childrenWithConditions = parent.getChildrenWithConditions();

        assertEquals(3, childrenWithConditions.size());

        assertEquals(child1, childrenWithConditions.get(0).getNode());
        assertEquals(condition1, childrenWithConditions.get(0).getCondition());

        assertEquals(child2, childrenWithConditions.get(1).getNode());
        assertEquals(condition2, childrenWithConditions.get(1).getCondition());

        assertEquals(defaultChild, childrenWithConditions.get(2).getNode());
        assertEquals(null, childrenWithConditions.get(2).getCondition());

        assertEquals(defaultChild, parent.getDefaultConditionalChild());

        assertEquals(Arrays.asList(child1, child2, defaultChild), parent.getAllChildren());
    }

    @Test
    public void testAddedAsParentWithoutCondition() {
        final N parent = getBuilderInstance().withName("parent").build();

        final N child1 = getBuilderInstance().withName("child1").withParent(parent).build();
        final N child2 = getBuilderInstance().withName("child2").withParent(parent).build();


        final List<Node> childrenWithoutConditions = parent.getChildrenWithoutConditions();

        assertEquals(Arrays.asList(child1, child2), childrenWithoutConditions);
    }

    @Test
    public void testAddedAsParentWithConditionWhenChildWithoutConditionExistsThrows() {
        final N parent = getBuilderInstance().build();

        final N child1 = getBuilderInstance()
                .withParent(parent)
                .build();
        final B child2builder = getBuilderInstance()
                .withParentWithCondition(parent, "any_condition");

        expectedException.expect(IllegalStateException.class);
        final N child2 = child2builder.build();
    }

    @Test
    public void testAddedAsParentWithoutConditionWhenChildWithConditionExistsThrows() {
        final N parent = getBuilderInstance().build();

        final N child1 = getBuilderInstance()
                .withParentWithCondition(parent, "any_condition")
                .build();
        final B child2builder = getBuilderInstance()
                .withParent(parent);

        expectedException.expect(IllegalStateException.class);
        final N child2 = child2builder.build();
    }

    @Test
    public void testClearParents() {
        final N parent1 = Mockito.spy(getBuilderInstance().build());
        final N parent2 = Mockito.spy(getBuilderInstance().build());
        final N parent3 = Mockito.spy(getBuilderInstance().build());

        final B builder = getBuilderInstance();
        builder.withParent(parent1)
                .withParent(parent2)
                .withParentWithCondition(parent3, "any_condition");

        builder.clearParents();

        final N child = builder.build();

        assertEquals(0, child.getAllParents().size());

        Mockito.verifyNoMoreInteractions(parent1);
        Mockito.verifyNoMoreInteractions(parent2);
        Mockito.verifyNoMoreInteractions(parent3);
    }

    @Test
    public void testNameAdded() {
        final B builder = getBuilderInstance();
        builder.withName(NAME);

        final N action = builder.build();
        assertEquals(NAME, action.getName());
    }

    @Test
    public void testNameAddedTwiceThrows() {
        final B builder = getBuilderInstance();
        builder.withName(NAME);

        expectedException.expect(IllegalStateException.class);
        builder.withName("any_name");
    }

    @Test
    public void testFromExistingNode() {
        final Node parent1 = new MapReduceActionBuilder().withName("parent1").build();
        final Node parent2 = new MapReduceActionBuilder().withName("parent2").build();
        final Node parent3 = new MapReduceActionBuilder().withName("parent3").build();
        final Node parent4 = new MapReduceActionBuilder().withName("parent4").build();

        final String condition = "condition";

        final B builder = getBuilderInstance();

        builder.withName(NAME)
                .withParent(parent1)
                .withParent(parent2)
                .withParentWithCondition(parent4, condition);

        final N node = builder.build();

        final B fromExistingBuilder = getBuilderInstance(node);

        final String newName = "fromExisting_" + NAME;
        fromExistingBuilder.withName(newName)
                .withoutParent(parent2)
                .withParent(parent3);

        final Node modifiedNode = fromExistingBuilder.build();

        assertEquals(newName, modifiedNode.getName());
        assertEquals(Arrays.asList(parent1, parent3, parent4), modifiedNode.getAllParents());
        assertEquals(Arrays.asList(parent1, parent3), modifiedNode.getParentsWithoutConditions());

        final List<Node.NodeWithCondition> parentsWithConditions = modifiedNode.getParentsWithConditions();
        assertEquals(1, parentsWithConditions.size());
        assertEquals(parent4, parentsWithConditions.get(0).getNode());
        assertEquals(condition, parentsWithConditions.get(0).getCondition());
    }
}
