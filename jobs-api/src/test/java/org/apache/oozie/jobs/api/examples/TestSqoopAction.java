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
import org.apache.oozie.jobs.api.GraphVisualization;
import org.apache.oozie.jobs.api.action.*;
import org.apache.oozie.jobs.api.oozie.dag.Graph;
import org.apache.oozie.jobs.api.serialization.Serializer;
import org.apache.oozie.jobs.api.workflow.Workflow;
import org.apache.oozie.jobs.api.workflow.WorkflowBuilder;
import org.apache.oozie.test.WorkflowTestCase;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class TestSqoopAction extends WorkflowTestCase {
    public void testForkedSqoopActions() throws IOException, JAXBException, OozieClientException {
        final Prepare prepare = new PrepareBuilder()
                .withDelete("hdfs://localhost:8020/user/${wf:user()}/examples/output")
                .build();

        final SqoopAction parent = SqoopActionBuilder.create()
                .withName("parent")
                .withJobTracker(getJobTrackerUri())
                .withNameNode(getNameNodeUri())
                .withPrepare(prepare)
                .withConfigProperty("mapred.job.queue.name", "default")
                .withCommand("python")
                .build();

        //  We are reusing the definition of parent and only modifying and adding what is different.
        final SqoopAction leftChild = SqoopActionBuilder.createFromExistingAction(parent)
                .withName("leftChild")
                .withParent(parent)
                .withCommand("python3")
                .build();

        final SqoopAction rightChild = SqoopActionBuilder.createFromExistingAction(leftChild)
                .withName("rightChild")
                .withoutArgument("arg2")
                .withArgument("arg3")
                .withCommand(null)
                .build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("simple-sqoop-example")
                .withDagContainingNode(parent).build();

        final String xml = Serializer.serialize(workflow);

        System.out.println(xml);

        GraphVisualization.workflowToPng(workflow, "simple-sqoop-example-workflow.png");

        final Graph intermediateGraph = new Graph(workflow);

        GraphVisualization.graphToPng(intermediateGraph, "simple-sqoop-example-graph.png");

        log.debug("Workflow XML is:\n{0}", xml);

        validate(xml);
    }
}
