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

import com.google.common.collect.ImmutableList;
import org.apache.oozie.jobs.api.action.Delete;
import org.apache.oozie.jobs.api.action.Mkdir;
import org.apache.oozie.jobs.api.action.Prepare;
import org.apache.oozie.jobs.api.generated.workflow.MKDIR;
import org.apache.oozie.jobs.api.generated.workflow.PREPARE;
import org.apache.oozie.jobs.api.generated.workflow.DELETE;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPrepareMapping {
    @Test
    public void testMappingPrepare() {
        final String deletePath1 = "path/to/delete/location1";
        final Boolean skipTrash1 = true;
        final Delete delete1 = new Delete(deletePath1, skipTrash1);

        final String deletePath2 = "path/to/delete/location2";
        final Boolean skipTrash2 = false;
        final Delete delete2 = new Delete(deletePath2, skipTrash2);

        final String mkdirPath1 = "path/to/mkdir/location1";
        final Mkdir mkdir1 = new Mkdir(mkdirPath1);

        final String mkdirPath2 = "path/to/mkdir/location2";
        final Mkdir mkdir2 = new Mkdir(mkdirPath2);

        final ImmutableList<Delete> deletes = ImmutableList.copyOf(Arrays.asList(delete1, delete2));
        final ImmutableList<Mkdir> mkdirs = ImmutableList.copyOf(Arrays.asList(mkdir1, mkdir2));

        final Prepare prepare = new Prepare(deletes, mkdirs);

        final PREPARE prepareJAXB = DozerMapperSingletonWrapper.getMapperInstance().map(prepare, PREPARE.class);

        final List<DELETE> deletesJAXB = prepareJAXB.getDelete();
        final DELETE delete1JAXB = deletesJAXB.get(0);
        final DELETE delete2JAXB = deletesJAXB.get(1);

        final List<MKDIR> mkdirsJAXB = prepareJAXB.getMkdir();
        final MKDIR mkdir1JAXB = mkdirsJAXB.get(0);
        final MKDIR mkdir2JAXB = mkdirsJAXB.get(1);

        assertEquals(deletePath1, delete1JAXB.getPath());
        assertEquals(skipTrash1, delete1JAXB.isSkipTrash());

        assertEquals(deletePath2, delete2JAXB.getPath());
        assertEquals(skipTrash2, delete2JAXB.isSkipTrash());

        assertEquals(mkdirPath1, mkdir1JAXB.getPath());
        assertEquals(mkdirPath2, mkdir2JAXB.getPath());
    }
}
