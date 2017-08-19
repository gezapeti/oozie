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

package org.apache.oozie.jobs.api.mapping;

import org.apache.oozie.jobs.api.Condition;
import org.apache.oozie.jobs.api.action.EmailActionBuilder;
import org.apache.oozie.jobs.api.action.ErrorHandler;
import org.apache.oozie.jobs.api.action.FSAction;
import org.apache.oozie.jobs.api.action.FSActionBuilder;
import org.apache.oozie.jobs.api.action.MapReduceAction;
import org.apache.oozie.jobs.api.action.MapReduceActionBuilder;
import org.apache.oozie.jobs.api.action.Node;
import org.apache.oozie.jobs.api.generated.workflow.ACTION;
import org.apache.oozie.jobs.api.generated.workflow.ACTIONTRANSITION;
import org.apache.oozie.jobs.api.generated.workflow.CASE;
import org.apache.oozie.jobs.api.generated.workflow.DECISION;
import org.apache.oozie.jobs.api.generated.workflow.DEFAULT;
import org.apache.oozie.jobs.api.generated.workflow.END;
import org.apache.oozie.jobs.api.generated.workflow.FORK;
import org.apache.oozie.jobs.api.generated.workflow.FORKTRANSITION;
import org.apache.oozie.jobs.api.generated.workflow.JOIN;
import org.apache.oozie.jobs.api.generated.workflow.KILL;
import org.apache.oozie.jobs.api.generated.workflow.ObjectFactory;
import org.apache.oozie.jobs.api.generated.workflow.START;
import org.apache.oozie.jobs.api.generated.workflow.SWITCH;
import org.apache.oozie.jobs.api.generated.workflow.WORKFLOWAPP;
import org.apache.oozie.jobs.api.oozie.dag.Decision;
import org.apache.oozie.jobs.api.oozie.dag.DecisionJoin;
import org.apache.oozie.jobs.api.oozie.dag.End;
import org.apache.oozie.jobs.api.oozie.dag.ExplicitNode;
import org.apache.oozie.jobs.api.oozie.dag.Fork;
import org.apache.oozie.jobs.api.oozie.dag.Graph;
import org.apache.oozie.jobs.api.oozie.dag.Join;
import org.apache.oozie.jobs.api.oozie.dag.Start;
import org.apache.oozie.jobs.api.workflow.Workflow;
import org.apache.oozie.jobs.api.workflow.WorkflowBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestGraphMapping {
    private static final ObjectFactory objectFactory = new ObjectFactory();

    @Test
    public void testMappingGraphFromWorkflow() {
        final String errorHandlerName = "error-handler";

        final EmailActionBuilder emailBuilder = EmailActionBuilder.create().withName(errorHandlerName)
                .withRecipient("somebody@workplace.com")
                .withSubject("Subject")
                .withBody("Email body.");

        final ErrorHandler errorHandler = ErrorHandler.buildAsErrorHandler(emailBuilder);

        final MapReduceAction mrAction = MapReduceActionBuilder.create()
                .withName("map-reduce-action")
                .build();
        final FSAction fsAction = FSActionBuilder.create()
                .withName("fs-action")
                .withParent(mrAction)
                .withErrorHandler(errorHandler)
                .build();

        final Workflow workflow = new WorkflowBuilder().withName("workflow-name").withDagContainingNode(mrAction).build();

        final Graph graph = new Graph(workflow);

        final WORKFLOWAPP workflowapp = DozerMapperSingletonWrapper.getMapperInstance().map(graph, WORKFLOWAPP.class);

        final WORKFLOWAPP expectedWorkflowapp = objectFactory.createWORKFLOWAPP();
        expectedWorkflowapp.setName(workflow.getName());

        final START start = objectFactory.createSTART();
        start.setTo(mrAction.getName());
        expectedWorkflowapp.setStart(start);

        final END end = objectFactory.createEND();
        end.setName("end");
        expectedWorkflowapp.setEnd(end);

        final List<Object> actions = expectedWorkflowapp.getDecisionOrForkOrJoin();

        final ACTION actionMr = convertEmailActionByHand((ExplicitNode) graph.getNodeByName(mrAction.getName()));

        final ACTION actionFs = convertEmailActionByHand((ExplicitNode) graph.getNodeByName(fsAction.getName()));

        final ACTIONTRANSITION error = actionFs.getError();
        error.setTo(errorHandlerName);

        final Node emailErrorHandlerNode = emailBuilder.build();
        final ExplicitNode emailErrorHandlerExplicitNode
                = new ExplicitNode(emailErrorHandlerNode.getName(), emailErrorHandlerNode);
        final ACTION errorHandlerAction = convertEmailActionByHand(emailErrorHandlerExplicitNode);

        final KILL kill = createKillNode();

        final ACTIONTRANSITION okAndError = objectFactory.createACTIONTRANSITION();
        okAndError.setTo(kill.getName());

        errorHandlerAction.setOk(okAndError);
        errorHandlerAction.setError(okAndError);

        actions.add(kill);
        actions.add(actionMr);
        actions.add(errorHandlerAction);
        actions.add(actionFs);

        // TODO: this is not ready.
        assertEquals(expectedWorkflowapp, workflowapp);
    }

    @Test
    public void testMappingGraphFromNodes() {
        final String workflowName = "test-workflow";
        final String condition = "condition";

        final ExplicitNode A = new ExplicitNode("A", EmailActionBuilder.create().build());
        final ExplicitNode B = new ExplicitNode("B", EmailActionBuilder.create().build());
        final ExplicitNode C = new ExplicitNode("C", EmailActionBuilder.create().build());
        final ExplicitNode D = new ExplicitNode("D", EmailActionBuilder.create().build());
        final ExplicitNode E = new ExplicitNode("E", EmailActionBuilder.create().build());

        final Start start = new Start("start");
        final End end = new End("end");
        final Fork fork = new Fork("fork1");
        final Join join = new Join("join1", fork);
        final Decision decision = new Decision("decision");
        final DecisionJoin decisionJoin = new DecisionJoin("decisionJoin", decision);

        end.addParent(join);
        join.addParent(decisionJoin);
        join.addParent(C);
        decisionJoin.addParent(D);
        decisionJoin.addParent(E);
        D.addParentWithCondition(decision, Condition.actualCondition(condition));
        E.addParentDefaultConditional(decision);
        decision.addParent(B);
        B.addParent(fork);
        C.addParent(fork);
        fork.addParent(A);
        A.addParent(start);

        final Nodes nodes = new Nodes(workflowName, start, end,
                Arrays.asList(A, B, C, D, E, fork, join, decision, decisionJoin));


        final WORKFLOWAPP expectedWorkflowApp = objectFactory.createWORKFLOWAPP();
        expectedWorkflowApp.setName(workflowName);

        final START startJaxb = objectFactory.createSTART();
        startJaxb.setTo(start.getChild().getName());
        expectedWorkflowApp.setStart(startJaxb);

        final END endJaxb = objectFactory.createEND();
        endJaxb.setName(end.getName());
        expectedWorkflowApp.setEnd(endJaxb);

        final List<Object> nodesInWorkflowApp = expectedWorkflowApp.getDecisionOrForkOrJoin();

        final FORK forkJaxb = objectFactory.createFORK();
        forkJaxb.setName(fork.getName());
        final List<FORKTRANSITION> transitions = forkJaxb.getPath();
        final FORKTRANSITION transitionB = objectFactory.createFORKTRANSITION();
        transitionB.setStart(B.getName());
        final FORKTRANSITION transitionC = objectFactory.createFORKTRANSITION();
        transitionC.setStart(C.getName());
        transitions.add(transitionB);
        transitions.add(transitionC);

        final ACTION actionA = convertEmailActionByHand(A);

        final ACTION actionB = convertEmailActionByHand(B);

        final ACTION actionC = convertEmailActionByHand(C);

        final DECISION decisionJaxb = objectFactory.createDECISION();
        decisionJaxb.setName(decision.getName());
        final SWITCH _switch = objectFactory.createSWITCH();
        final List<CASE> cases = _switch.getCase();
        final CASE _case = objectFactory.createCASE();
        _case.setTo(D.getName());
        _case.setValue(condition);
        cases.add(_case);
        final DEFAULT _default = objectFactory.createDEFAULT();
        _default.setTo(E.getName());
        _switch.setDefault(_default);
        decisionJaxb.setSwitch(_switch);

        final ACTION actionD = convertEmailActionByHand(D);

        final ACTION actionE = convertEmailActionByHand(E);

        final JOIN joinJaxb = objectFactory.createJOIN();
        joinJaxb.setName(join.getName());
        joinJaxb.setTo(end.getName());

        // TODO: Unfortunately the order of the elements counts.
        nodesInWorkflowApp.add(createKillNode());
        nodesInWorkflowApp.add(actionA);
        nodesInWorkflowApp.add(actionB);
        nodesInWorkflowApp.add(actionC);
        nodesInWorkflowApp.add(actionD);
        nodesInWorkflowApp.add(actionE);
        nodesInWorkflowApp.add(forkJaxb);
        nodesInWorkflowApp.add(joinJaxb);
        nodesInWorkflowApp.add(decisionJaxb);

        final WORKFLOWAPP workflowapp = DozerMapperSingletonWrapper.getMapperInstance().map(nodes, WORKFLOWAPP.class);
        // TODO: Not equal because JAXBElement doesn't override equals.
        assertEquals(expectedWorkflowApp, workflowapp);
    }

    private ACTION convertEmailActionByHand(final ExplicitNode node) {
        final ACTION action = DozerMapperSingletonWrapper.getMapperInstance().map(node, ACTION.class);

        final ACTIONTRANSITION error = objectFactory.createACTIONTRANSITION();
        error.setTo(createKillNode().getName());
        action.setError(error);

        return action;
    }

    private KILL createKillNode() {
        final KILL kill = objectFactory.createKILL();
        kill.setName("kill");
        kill.setMessage("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");

        return kill;
    }
}