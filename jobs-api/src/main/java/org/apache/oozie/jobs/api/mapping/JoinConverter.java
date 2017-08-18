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

import org.apache.oozie.jobs.api.generated.workflow.JOIN;
import org.apache.oozie.jobs.api.generated.workflow.ObjectFactory;
import org.apache.oozie.jobs.api.oozie.dag.Join;
import org.apache.oozie.jobs.api.oozie.dag.NodeBase;
import org.dozer.DozerConverter;

public class JoinConverter extends DozerConverter<Join, JOIN> {
    public JoinConverter() {
        super(Join.class, JOIN.class);
    }

    @Override
    public JOIN convertTo(Join source, JOIN destination) {
        if (destination == null) {
            destination = new ObjectFactory().createJOIN();
        }

        destination.setName(source.getName());

        final NodeBase child  = source.getChild();
        final NodeBase realChild = MappingUtils.getRealChild(child);

        destination.setTo(realChild.getName());

        return destination;
    }

    @Override
    public Join convertFrom(JOIN source, Join destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
