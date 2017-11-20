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

package org.apache.oozie.jobs.api.examples;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.jobs.api.GraphVisualization;
import org.apache.oozie.jobs.api.action.*;
import org.apache.oozie.jobs.api.oozie.dag.Graph;
import org.apache.oozie.jobs.api.serialization.Serializer;
import org.apache.oozie.jobs.api.workflow.Workflow;
import org.apache.oozie.jobs.api.workflow.WorkflowBuilder;
import org.apache.oozie.test.TestWorkflow;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class TestDistcpAction extends TestWorkflow {
    public void testForkedDistcpActions() throws IOException, JAXBException, OozieClientException {
        final Prepare prepare = new PrepareBuilder()
                .withDelete("hdfs://localhost:8020/user/${wf:user()}/examples/output")
                .build();

        final DistcpAction parent = DistcpActionBuilder.create()
                .withName("parent")
                .withJobTracker(getJobTrackerUri())
                .withNameNode(getNameNodeUri())
                .withPrepare(prepare)
                .withConfigProperty("mapred.job.queue.name", "default")
                .withJavaOpts("-Dopt1 -Dopt2")
                .withArg("arg1")
                .build();

        //  We are reusing the definition of parent and only modifying and adding what is different.
        final DistcpAction leftChild = DistcpActionBuilder.createFromExistingAction(parent)
                .withName("leftChild")
                .withParent(parent)
                .withoutArg("arg1")
                .withArg("arg2")
                .build();

        final DistcpAction rightChild = DistcpActionBuilder.createFromExistingAction(leftChild)
                .withName("rightChild")
                .withoutArg("arg2")
                .withArg("arg3")
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("simple-distcp-example")
                .withDagContainingNode(parent).build();

        final String xml = Serializer.serialize(workflow);

        System.out.println(xml);

        GraphVisualization.workflowToPng(workflow, "simple-distcp-example-workflow.png");

        final Graph intermediateGraph = new Graph(workflow);

        GraphVisualization.graphToPng(intermediateGraph, "simple-distcp-example-graph.png");

        log.debug("Workflow XML is:\n{0}", xml);

        submitAndAssert(xml, WorkflowJob.Status.KILLED);
    }
}
