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

import com.google.common.collect.ImmutableList;
import org.apache.oozie.jobs.api.action.Node;
import org.apache.oozie.jobs.api.workflow.Workflow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Graph {
    private final String name;
    private final Start start = new Start("start");
    private final End end = new End("end");
    private final Map<String, NodeBase> nodesByName = new LinkedHashMap<>();
    private final Map<Fork, Integer> forkNumbers = new HashMap<>();
    private int forkCounter = 1;

    private final Map<NodeBase, Decision> originalParentToCorrespondingDecision = new HashMap<>();
    private final Map<Decision, Integer> closedPathsOfDecisionNodes = new HashMap<>();
    private int decisionCounter = 1;
    private int decisionJoinCounter = 1;

    /**
     * Nodes that have a join downstream to them are closed, they should never get new children.
     */
    private final Map<NodeBase, Join> closingJoins = new HashMap<>();

    public Graph(final Workflow workflow) {
        this.name = workflow.getName();

        final List<Node> nodesInTopologicalOrder = getNodesInTopologicalOrder(workflow);

        storeNode(start);
        storeNode(end);

        convert(nodesInTopologicalOrder);
    }

    public String getName() {
        return name;
    }

    public Start getStart() {
        return start;
    }

    public End getEnd() {
        return end;
    }

    public NodeBase getNodeByName(final String name) {
        return nodesByName.get(name);
    }

    public Collection<NodeBase> getNodes() {
        return nodesByName.values();
    }

    private void convert(final List<Node> nodesInTopologicalOrder) {
        final Map<Node, NodeBase> nodeToNodeBase = new HashMap<>();

        for (final Node originalNode : nodesInTopologicalOrder) {
            final ExplicitNode convertedNode = new ExplicitNode(originalNode.getName(), originalNode);
            nodeToNodeBase.put(originalNode, convertedNode);
            storeNode(convertedNode);

            if (!originalNode.getChildrenWithConditions().isEmpty()) {
                // We insert a decision node below the current convertedNode.
                final Decision decision = newDecision();
                decision.addParent(convertedNode);
                originalParentToCorrespondingDecision.put(convertedNode, decision);
            }

            final List<DagNodeWithCondition> mappedParentsWithConditions = new ArrayList<>();
            for (final Node.NodeWithCondition parentNodeWithCondition : originalNode.getParentsWithConditions()) {
                final NodeBase mappedParentNode = nodeToNodeBase.get(parentNodeWithCondition.getNode());
                final String condition = parentNodeWithCondition.getCondition();
                DagNodeWithCondition parentNodeBaseWithCondition
                        = new DagNodeWithCondition(mappedParentNode,
                                                   // Null means a default condition in Node objects.
                                                   condition == null ? DagNodeWithCondition.DEFAULT_CONDITION : condition);
                mappedParentsWithConditions.add(parentNodeBaseWithCondition);
            }

            for (final Node parent : originalNode.getParentsWithoutConditions()) {
                mappedParentsWithConditions.add(new DagNodeWithCondition(nodeToNodeBase.get(parent), null));
            }

            handleNodeWithParents(mappedParentsWithConditions, convertedNode);
        }

        final List<DagNodeWithCondition> finalNodes = new ArrayList<>();
        for (final NodeBase maybeFinalNode : nodesByName.values()) {
            final boolean hasNoChildrenAndIsNotEnd = maybeFinalNode.getChildren().isEmpty() && maybeFinalNode != end;
            if (hasNoChildrenAndIsNotEnd) {
                finalNodes.add(new DagNodeWithCondition(maybeFinalNode, null));
            }
        }

        handleNodeWithParents(finalNodes, end);
    }

    private void storeNode(final NodeBase node) {
        final String name = node.getName();

        final boolean isPresent = nodesByName.containsKey(name);
        if (isPresent) {
            final String errorMessage = String.format("Duplicate name '%s' found in graph '%s'", node.getName(), this.getName());
            throw new IllegalArgumentException(errorMessage);
        }

        nodesByName.put(node.getName(), node);
    }

    private NodeBase getNewParent(final NodeBase originalParent) {
        NodeBase newParent = originalParent;

        if (originalParentToCorrespondingDecision.containsKey(newParent)) {
            newParent = originalParentToCorrespondingDecision.get(newParent);
        }

        newParent = getNearestNonClosedDescendant(newParent);

        return newParent;
    }

    private void handleNodeWithParents(final List<DagNodeWithCondition> parentsWithConditions, final NodeBase node) {
        // Avoiding adding children to nodes that are inside a closed fork / join pair and to original parents of decision nodes.
        final List<DagNodeWithCondition> newParentsWithConditions = new ArrayList<>();
        for (final DagNodeWithCondition parentWithCondition : parentsWithConditions) {
            final NodeBase parent = parentWithCondition.getNode();
            final String condition = parentWithCondition.getCondition();

            final NodeBase newParent = getNewParent(parent);
            final DagNodeWithCondition newParentWithCondition = new DagNodeWithCondition(newParent, condition);

            if (!newParentsWithConditions.contains(newParentWithCondition)) {
                newParentsWithConditions.add(newParentWithCondition);
            }
        }

        if (newParentsWithConditions.isEmpty()) {
            handleSingleParentNode(new DagNodeWithCondition(start, null), node);
        }
        else if (newParentsWithConditions.size() == 1) {
            handleSingleParentNode(newParentsWithConditions.get(0), node);
        }
        else {
            handleMultiParentNodeWithParents(node, newParentsWithConditions);
        }
    }

    private void handleSingleParentNode(final DagNodeWithCondition parentWithCondition, final NodeBase node) {
        addParentWithForkIfNeeded(node, parentWithCondition);
    }

    private void handleMultiParentNodeWithParents(final NodeBase node, final List<DagNodeWithCondition> parentsWithConditions) {
        final List<PathInformation> paths = new ArrayList<>();
        for (final DagNodeWithCondition parentWithCondition : parentsWithConditions) {
            final NodeBase parent = parentWithCondition.getNode();
            paths.add(getPathInfo(parent));
        }

        final BranchingToClose toClose = chooseBranchingToClose(paths);

        // Eliminating redundant parents.
        if (toClose.isRedundantParent()) {
            final List<DagNodeWithCondition> parentsWithoutRedundant = new ArrayList<>(parentsWithConditions);
            DagNodeWithCondition.removeFromCollection(parentsWithoutRedundant, toClose.getRedundantParent());

            handleNodeWithParents(parentsWithoutRedundant, node);
        }
        else if (toClose.isDecision()) {
            insertDecisionJoin(node, parentsWithConditions, toClose);
        }
        else {
            insertJoin(parentsWithConditions, node, toClose);
        }
    }

    private void insertDecisionJoin(final NodeBase node,
                                    final List<DagNodeWithCondition> parentsWithConditions,
                                    final BranchingToClose branchingToClose) {
        final Decision decision = branchingToClose.getDecision();
        final DecisionJoin decisionJoin = newDecisionJoin(decision, branchingToClose.getPaths().size());

        for (DagNodeWithCondition parentWithCondition : parentsWithConditions) {
            addParentWithForkIfNeeded(decisionJoin, parentWithCondition);
        }

        addParentWithForkIfNeeded(node, new DagNodeWithCondition(decisionJoin, null));
    }

    private void insertJoin(final List<DagNodeWithCondition> parentsWithConditions,
                            final NodeBase node,
                            final BranchingToClose branchingToClose) {
        if (branchingToClose.isSplittingJoinNeeded()) {
            // We have to close a subset of the paths.
            final List<DagNodeWithCondition> newParentsWithConditions = new ArrayList<>(parentsWithConditions);
            final List<NodeBase> parentsInToClose = new ArrayList<>();

            for (final PathInformation path : branchingToClose.getPaths()) {
                parentsInToClose.add(path.getBottom());
                DagNodeWithCondition.removeFromCollection(newParentsWithConditions, path.getBottom());
            }

            final Join newJoin = joinPaths(branchingToClose.getFork(), branchingToClose.getPaths());

            newParentsWithConditions.add(new DagNodeWithCondition(newJoin, null));

            handleMultiParentNodeWithParents(node, newParentsWithConditions);
        }
        else {
            // There are no intermediary fork / join pairs to insert, we have to join all paths in a single join.
            final Join newJoin = joinPaths(branchingToClose.getFork(), branchingToClose.getPaths());

            if (newJoin != null) {
                addParentWithForkIfNeeded(node, new DagNodeWithCondition(newJoin, null));
            }
            else {
                // Null means a part of the paths was relocated because of a decision node.
                handleNodeWithParents(parentsWithConditions, node);
            }
        }
    }

    // Returning null means we have relocated a part of the paths because of decision nodes, so the caller should try
    // adding the node again.
    private Join joinPaths(final Fork fork, final List<PathInformation> pathsToJoin) {
        final Map<PathInformation, Decision> highestDecisionNodes = new LinkedHashMap<>();
        for (final PathInformation path : pathsToJoin) {
            for (int ixNodeOnPath = 0; ixNodeOnPath < path.getNodes().size(); ++ixNodeOnPath) {
                final NodeBase nodeOnPath = path.getNodes().get(ixNodeOnPath);

                if (nodeOnPath instanceof Decision) {
                    // Excluding decision nodes where no branch goes out of this fork / join pair.
                    if (!isDecisionClosed((Decision) nodeOnPath)) {
                        highestDecisionNodes.put(path, (Decision) nodeOnPath);
                    }
                }
                else if (nodeOnPath == fork) {
                    break;
                }
            }
        }

        if (highestDecisionNodes.isEmpty()) {
            return joinPathsWithoutDecisions(fork, pathsToJoin);
        }
        else {
            return joinPathsWithDecisions(fork, pathsToJoin, highestDecisionNodes);
        }
    }

    private Join joinPathsWithoutDecisions(final Fork fork, final List<PathInformation> pathsToJoin) {
        final Set<NodeBase> mainBranchNodes = new LinkedHashSet<>();
        for (final PathInformation pathInformation : pathsToJoin) {
            mainBranchNodes.addAll(pathInformation.getNodes());
        }

        // Taking care of side branches.
        final Set<NodeBase> closedNodes = new HashSet<>();
        final List<NodeBase> sideBranches = new ArrayList<>();
        for (final PathInformation path : pathsToJoin) {
            for (int ixNodeOnPath = 0; ixNodeOnPath < path.getNodes().size(); ++ixNodeOnPath) {
                final NodeBase nodeOnPath = path.getNodes().get(ixNodeOnPath);

                if (nodeOnPath == fork) {
                    break;
                }

                if (nodeOnPath instanceof Decision && isDecisionClosed((Decision) nodeOnPath)) {
                    break;
                }

                sideBranches.addAll(cutSideBranches(nodeOnPath, mainBranchNodes));
                closedNodes.add(nodeOnPath);
            }
        }

        final Join newJoin;

        // Check if we have to divide the fork.
        final boolean hasMoreForkedChildren = pathsToJoin.size() < fork.getChildren().size();
        if (hasMoreForkedChildren) {
            // Dividing the fork.
            newJoin = divideForkAndCloseSubFork(fork, pathsToJoin);
        } else {
            // We don't divide the fork.
            newJoin = newJoin(fork);

            for (final PathInformation path : pathsToJoin) {
                addParentWithForkIfNeeded(newJoin, new DagNodeWithCondition(path.getBottom(), null));
            }
        }

        // Inserting the side branches under the new join node.
        for (final NodeBase sideBranch : sideBranches) {
            addParentWithForkIfNeeded(sideBranch, new DagNodeWithCondition(newJoin, null));
        }

        // Marking the nodes as closed.
        for (final NodeBase closedNode : closedNodes) {
            markAsClosed(closedNode, newJoin);
        }

        return newJoin;
    }

    private Join joinPathsWithDecisions(final Fork fork,
                                        final List<PathInformation> pathsToJoin,
                                        final Map<PathInformation, Decision> highestDecisionNodes) {
        final Set<Decision> decisions = new HashSet<>(highestDecisionNodes.values());

        final List<PathInformation> newPaths = new ArrayList<>();
        boolean shouldCloseJoinAndAddOtherDecisionsUnderIt = false;
        for (Decision decision : decisions) {
            final NodeBase parentOfDecision = decision.getParent();

            if (parentOfDecision == fork) {
                shouldCloseJoinAndAddOtherDecisionsUnderIt = true;
                break;
            }

            newPaths.add(getPathInfo(parentOfDecision));
            removeParentWithForkIfNeeded(decision, decision.getParent());
        }

        if (shouldCloseJoinAndAddOtherDecisionsUnderIt) {
            closeJoinAndAddOtherDecisionsUnderIt(fork, decisions);
        }
        else {
            for (PathInformation path : pathsToJoin) {
                if (!highestDecisionNodes.containsKey(path)) {
                    newPaths.add(path);
                }
            }

            final Join newJoin = joinPaths(fork, newPaths);

            for (Decision decision : decisions) {
                addParentWithForkIfNeeded(decision, new DagNodeWithCondition(newJoin, null));
            }
        }

        return null;
    }

    private void closeJoinAndAddOtherDecisionsUnderIt(final Fork fork, final Set<Decision> decisions) {
        // TODO: Either implement it correctly or give a more informative error message.
        throw new IllegalStateException("Conditional paths originating ultimately from the same parallel branching (fork) do not converge to the same join.");
    }

    private void markAsClosed(final NodeBase node, final Join join) {
        closingJoins.put(node, join);
    }

    private List<NodeBase> cutSideBranches(final NodeBase node,
                                           final Set<NodeBase> mainBranchNodes) {
        final List<NodeBase> sideBranches = new ArrayList<>();

        // Closed forks cannot have side branches.
        final boolean isClosedFork = node instanceof Fork && ((Fork) node).isClosed();
        if (!isClosedFork) {
            for (final NodeBase childOfForkOrParent : node.getChildren()) {
                if (!mainBranchNodes.contains(childOfForkOrParent)) {
                    removeParentWithForkIfNeeded(childOfForkOrParent, node);
                    sideBranches.add(childOfForkOrParent);
                }
            }
        }

        return sideBranches;
    }

    private Join divideForkAndCloseSubFork(final Fork correspondingFork,
                                           final List<PathInformation> paths) {
        final Fork newFork = newFork();
        for (final PathInformation path : paths) {
            final int indexOfFork = path.getNodes().indexOf(correspondingFork);
            final NodeBase childOfOriginalFork = path.getNodes().get(indexOfFork - 1);

            childOfOriginalFork.removeParent(correspondingFork);
            childOfOriginalFork.addParent(newFork);
        }

        newFork.addParent(correspondingFork);

        final Join newJoin = newJoin(newFork);

        for (final PathInformation path : paths) {
            newJoin.addParent(path.getBottom());
        }

        return newJoin;
    }

    private BranchingToClose chooseBranchingToClose(final List<PathInformation> paths) {
        int maxPathLength = 0;
        for (final PathInformation pathInformation : paths) {
            if (maxPathLength < pathInformation.getNodes().size()) {
                maxPathLength = pathInformation.getNodes().size();
            }
        }

        for (int ixLevel = 0; ixLevel < maxPathLength; ++ixLevel) {
            final BranchingToClose foundAtThisLevel = chooseBranchingToClose(paths, ixLevel);

            if (foundAtThisLevel != null) {
                return foundAtThisLevel;
            }
        }

        throw new IllegalStateException("We should never reach here.");
    }

    private BranchingToClose chooseBranchingToClose(final List<PathInformation> paths, final int ixLevel) {
        for (final PathInformation path : paths) {
            if (ixLevel < path.getNodes().size()) {
                final NodeBase branching = path.getNodes().get(ixLevel);

                final List<PathInformation> pathsMeetingAtCurrentFork = getPathsContainingNode(branching, paths);

                if (pathsMeetingAtCurrentFork.size() > 1) {
                    final boolean needToSplitJoin = pathsMeetingAtCurrentFork.size() < paths.size();

                    // If branching is not a Fork or a Decision, then it is a redundant parent.
                    if (branching instanceof Fork) {
                        return BranchingToClose.withFork((Fork) branching, pathsMeetingAtCurrentFork, needToSplitJoin);
                    } else if (branching instanceof Decision) {
                        return BranchingToClose.withDecision((Decision) branching, pathsMeetingAtCurrentFork, needToSplitJoin);
                    }
                    else {
                        return BranchingToClose.withRedundantParent(branching, pathsMeetingAtCurrentFork, needToSplitJoin);
                    }
                }
            }
        }

        return null;
    }

    private List<PathInformation> getPathsContainingNode(final NodeBase node, final List<PathInformation> paths) {
        final List<PathInformation> pathsContainingNode = new ArrayList<>();

        for (final PathInformation pathInformationMaybeContaining : paths) {
            if (pathInformationMaybeContaining.getNodes().contains(node)) {
                pathsContainingNode.add(pathInformationMaybeContaining);
            }
        }

        return pathsContainingNode;
    }

    private PathInformation getPathInfo(final NodeBase node) {
        NodeBase current = node;

        final List<NodeBase> nodes = new ArrayList<>();

        while (current != start) {
            nodes.add(current);

            if (current instanceof Join) {
                // Get the fork pair of this join and go towards that
                final Fork forkPair = ((Join) current).getBranchingPair();
                current = forkPair;
            }
            else if (current instanceof DecisionJoin) {
                final Decision decisionPair = ((DecisionJoin) current).getBranchingPair();
                current = decisionPair;
            }
            else {
                current = getSingleParent(current);
            }
        }

        return new PathInformation(nodes);
    }

    private boolean isDecisionClosed(final Decision decision) {
        Integer closedPathsOfDecisionNode = closedPathsOfDecisionNodes.get(decision);
        return closedPathsOfDecisionNode != null && decision.getChildren().size() == closedPathsOfDecisionNode;
    }

    private NodeBase getSingleParent(final NodeBase node) {
        if (node instanceof End) {
            return ((End) node).getParent();
        }
        else if (node instanceof Fork) {
            return ((Fork) node).getParent();
        }
        else if (node instanceof Decision) {
            return ((Decision) node).getParent();
        }
        else if (node instanceof ExplicitNode) {
            return ((ExplicitNode) node).getParent();
        }
        else if (node instanceof Start) {
            throw new IllegalStateException("Start nodes have no parent.");
        }
        else if (node instanceof Join) {
            final Join join = (Join) node;
            final int numberOfParents = join.getParents().size();
            if (numberOfParents != 1) {
                throw new IllegalStateException("The join node called '" + node.getName()
                        + "' has " + numberOfParents + " parents instead of 1.");
            }

            return join.getParents().get(0);
        }
        else if (node == null) {
            throw new IllegalArgumentException("Null node found.");
        }

        throw new IllegalArgumentException("Unknown node type.");
    }

    // Returns the first descendant that is not inside a closed fork / join pair.
    private NodeBase getNearestNonClosedDescendant(final NodeBase node) {
        NodeBase current = node;

        while (closingJoins.containsKey(current)) {
            current = closingJoins.get(current);
        }

        return current;
    }

    private void addParentWithForkIfNeeded(final NodeBase node, final DagNodeWithCondition parentWithCondition) {
        final NodeBase parent = parentWithCondition.getNode();
        final String condition = parentWithCondition.getCondition();
        if (parent.getChildren().isEmpty() || parent instanceof Fork || parent instanceof Decision) {
            if (condition != null) {
                if (!(parent instanceof Decision)) {
                    throw new IllegalStateException("Trying to add a conditional parent that is not a decision.");
                }

                node.addParentWithCondition((Decision) parent, condition);
            }
            else {
                node.addParent(parent);
            }
        }
        else {
            // If there is no child, we never get to this point.
            // There is only one child, otherwise it is a fork and we don't get here.
            final NodeBase child = parent.getChildren().get(0);

            if (child instanceof Fork) {
                node.addParent(child);
            }
            else if (child instanceof Join) {
                addParentWithForkIfNeeded(node, new DagNodeWithCondition(child, null));
            }
            else {
                final Fork newFork = newFork();

                child.removeParent(parent);
                child.addParent(newFork);
                node.addParent(newFork);
                newFork.addParent(parent);
            }
        }
    }

    private void removeParentWithForkIfNeeded(final NodeBase node, final NodeBase parent) {
        node.removeParent(parent);

        final boolean isParentForkAndHasOneChild = parent instanceof Fork && parent.getChildren().size() == 1;
        if (isParentForkAndHasOneChild) {
            final NodeBase grandparent = ((Fork) parent).getParent();
            final NodeBase child = parent.getChildren().get(0);

            removeParentWithForkIfNeeded(parent, grandparent);
            child.removeParent(parent);
            child.addParent(grandparent);
            nodesByName.remove(parent.getName());
        }
    }

    private Fork newFork() {
        final Fork fork = new Fork("fork" + forkCounter);

        forkNumbers.put(fork, forkCounter);
        forkCounter++;

        storeNode(fork);

        return fork;
    }

    private Join newJoin(final Fork correspondingFork) {
        final Join join = new Join("join" + forkNumbers.get(correspondingFork), correspondingFork);

        storeNode(join);

        return join;
    }

    private Decision newDecision() {
        final Decision decision = new Decision("decision" + decisionCounter);

        decisionCounter++;

        storeNode(decision);

        return decision;
    }

    private DecisionJoin newDecisionJoin(final Decision correspondingDecision, final int numberOfPathsClosed) {
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin" + decisionJoinCounter, correspondingDecision);

        Integer numberOfAlreadyClosedChildren = closedPathsOfDecisionNodes.get(correspondingDecision);
        int newNumber = numberOfPathsClosed + (numberOfAlreadyClosedChildren == null ? 0 : numberOfAlreadyClosedChildren);

        closedPathsOfDecisionNodes.put(correspondingDecision, newNumber);

        decisionJoinCounter++;

        storeNode(decisionJoin);

        return decisionJoin;
    }

    private static List<Node> getNodesInTopologicalOrder(final Workflow workflow) {
        final List<Node> nodes = new ArrayList<>(workflow.getRoots());

        for (int i = 0; i < nodes.size(); ++i) {
            final Node current  = nodes.get(i);

            for (final Node child : current.getAllChildren()) {
                // Checking if every dependency has been processed, if not, we do not add the start to the list.
                final List<Node> dependencies = child.getAllParents();
                if (nodes.containsAll(dependencies) && !nodes.contains(child)) {
                    nodes.add(child);
                }
            }
        }

        return nodes;
    }

    private static class PathInformation {
        private final ImmutableList<NodeBase> nodes;

        PathInformation(final List<NodeBase> nodes) {
            this.nodes = new ImmutableList.Builder<NodeBase>().addAll(nodes).build();
        }

        NodeBase getBottom() {
            return nodes.get(0);
        }

        public List<NodeBase> getNodes() {
            return nodes;
        }

    }

    private static class BranchingToClose {
        private final Fork fork;
        private final Decision decision;
        private final NodeBase redundantParent;
        private final ImmutableList<PathInformation> paths;
        private final boolean needToSplitJoin;

        static BranchingToClose withFork(final Fork fork,
                                         final List<PathInformation> paths,
                                         final boolean needToSplitJoin) {
            return new BranchingToClose(fork, null, null, paths, needToSplitJoin);
        }

        static BranchingToClose withDecision(final Decision decision,
                                             final List<PathInformation> paths,
                                             final boolean needToSplitJoin) {
            return new BranchingToClose(null, decision, null, paths, needToSplitJoin);
        }

        static BranchingToClose withRedundantParent(final NodeBase redundantParent,
                                                    final List<PathInformation> paths,
                                                    final boolean needToSplitJoin) {
            return new BranchingToClose(null, null, redundantParent, paths, needToSplitJoin);
        }

        private BranchingToClose(final Fork fork,
                                 final Decision decision,
                                 final NodeBase redundantParent,
                                 final List<PathInformation> paths,
                                 final boolean needToSplitJoin) {
            checkOnlyOneIsNotNull(fork, decision, redundantParent);

            this.fork = fork;
            this.decision = decision;
            this.redundantParent = redundantParent;
            this.paths = ImmutableList.copyOf(paths);
            this.needToSplitJoin = needToSplitJoin;
        }

        public Fork getFork() {
            return fork;
        }

        public Decision getDecision() {
            return decision;
        }

        NodeBase getRedundantParent() {
            return redundantParent;
        }

        List<PathInformation> getPaths() {
            return paths;
        }

        boolean isFork() {
            return fork != null;
        }

        boolean isDecision() {
            return decision != null;
        }

        boolean isRedundantParent() {
            return redundantParent != null;
        }

        boolean isSplittingJoinNeeded() {
            return needToSplitJoin;
        }

        private void checkOnlyOneIsNotNull(final Fork fork, final Decision decision, final NodeBase redundantParent) {
            int counter = 0;

            if (fork != null) {
                ++counter;
            }

            if (decision != null) {
                ++counter;
            }

            if (redundantParent != null) {
                ++counter;
            }

            if (counter != 1) {
                throw new IllegalArgumentException("Exactly one of 'fork' and 'redundantParent' must be non-null.");
            }
        }
    }
}