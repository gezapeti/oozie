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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.oozie.jobs.api.ModifyOnce;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ActionAttributesBuilder {
    private final ModifyOnce<String> jobTracker;
    private final ModifyOnce<String> nameNode;
    private final ModifyOnce<Prepare> prepare;
    private final ModifyOnce<Streaming> streaming;
    private final ModifyOnce<Pipes> pipes;
    private final List<String> jobXmls;
    private final Map<String, ModifyOnce<String>> configuration;
    private final ModifyOnce<String> configClass;
    private final List<String> files;
    private final List<String> archives;

    private final List<Delete> deletes;
    private final List<Mkdir> mkdirs;
    private final List<Move> moves;
    private final List<Chmod> chmods;
    private final List<Touchz> touchzs;
    private final List<Chgrp> chgrps;
    private final List<Setrep> setreps;

    public static ActionAttributesBuilder create() {
        final ModifyOnce<String> jobTracker = new ModifyOnce<>();
        final ModifyOnce<String> nameNode = new ModifyOnce<>();
        final ModifyOnce<Prepare> prepare = new ModifyOnce<>();
        final ModifyOnce<Streaming> streaming = new ModifyOnce<>();
        final ModifyOnce<Pipes> pipes = new ModifyOnce<>();
        final List<String> jobXmls = new ArrayList<>();
        final Map<String, ModifyOnce<String>> configuration = new LinkedHashMap<>();
        final ModifyOnce<String> configClass = new ModifyOnce<>();
        final List<String> files = new ArrayList<>();
        final List<String> archives = new ArrayList<>();

        final List<Delete> deletes = new ArrayList<>();
        final List<Mkdir> mkdirs = new ArrayList<>();
        final List<Move> moves = new ArrayList<>();
        final List<Chmod> chmods = new ArrayList<>();
        final List<Touchz> touchzs = new ArrayList<>();
        final List<Chgrp> chgrps = new ArrayList<>();
        final List<Setrep> setreps = new ArrayList<>();

        return new ActionAttributesBuilder(
                jobTracker,
                nameNode,
                prepare,
                streaming,
                pipes,
                jobXmls,
                configuration,
                configClass,
                files,
                archives,

                deletes,
                mkdirs,
                moves,
                chmods,
                touchzs,
                chgrps,
                setreps);
    }

    public static ActionAttributesBuilder createFromExisting(final ActionAttributes attributes) {
        final ModifyOnce<String> jobTracker = new ModifyOnce<>(attributes.getJobTracker());
        final ModifyOnce<String> nameNode = new ModifyOnce<>(attributes.getNameNode());
        final ModifyOnce<Prepare> prepare = new ModifyOnce<>(attributes.getPrepare());
        final ModifyOnce<Streaming> streaming = new ModifyOnce<>(attributes.getStreaming());
        final ModifyOnce<Pipes> pipes = new ModifyOnce<>(attributes.getPipes());
        final List<String> jobXmls = new ArrayList<>(attributes.getJobXmls());
        final Map<String, ModifyOnce<String>> configuration = getModifiedOnceMap(attributes.getConfiguration());
        final ModifyOnce<String> configClass = new ModifyOnce<>(attributes.getConfigClass());
        final List<String> files = new ArrayList<>(attributes.getFiles());
        final List<String> archives = new ArrayList<>(attributes.getArchives());

        final List<Delete> deletes = new ArrayList<>(attributes.getDeletes());
        final List<Mkdir> mkdirs = new ArrayList<>(attributes.getMkdirs());
        final List<Move> moves = new ArrayList<>(attributes.getMoves());
        final List<Chmod> chmods = new ArrayList<>(attributes.getChmods());
        final List<Touchz> touchzs = new ArrayList<>(attributes.getTouchzs());
        final List<Chgrp> chgrps = new ArrayList<>(attributes.getChgrps());
        final List<Setrep> setreps = new ArrayList<>(attributes.getSetreps());

        return new ActionAttributesBuilder(
                jobTracker,
                nameNode,
                prepare,
                streaming,
                pipes,
                jobXmls,
                configuration,
                configClass,
                files,
                archives,

                deletes,
                mkdirs,
                moves,
                chmods,
                touchzs,
                chgrps,
                setreps);
    }

    ActionAttributesBuilder(final ModifyOnce<String> jobTracker,
                            final ModifyOnce<String> nameNode,
                            final ModifyOnce<Prepare> prepare,
                            final ModifyOnce<Streaming> streaming,
                            final ModifyOnce<Pipes> pipes,
                            final List<String> jobXmls,
                            final Map<String, ModifyOnce<String>> configuration,
                            final ModifyOnce<String> configClass,
                            final List<String> files,
                            final List<String> archives,

                            final List<Delete> deletes,
                            final List<Mkdir> mkdirs,
                            final List<Move> moves,
                            final List<Chmod> chmods,
                            final List<Touchz> touchzs,
                            final List<Chgrp> chgrps,
                            final List<Setrep> setreps) {
        this.jobTracker = jobTracker;
        this.nameNode = nameNode;
        this.prepare = prepare;
        this.streaming = streaming;
        this.pipes = pipes;
        this.jobXmls = jobXmls;
        this.configuration = configuration;
        this.configClass = configClass;
        this.files = files;
        this.archives = archives;

        this.deletes = deletes;
        this.mkdirs = mkdirs;
        this.moves = moves;
        this.chmods = chmods;
        this.touchzs = touchzs;
        this.chgrps = chgrps;
        this.setreps = setreps;
    }

    public void withJobTracker(final String jobTracker) {
        this.jobTracker.set(jobTracker);
    }

    public void withNameNode(final String nameNode) {
        this.nameNode.set(nameNode);
    }

    public void withPrepare(final Prepare prepare) {
        this.prepare.set(prepare);
    }

    public void withStreaming(final Streaming streaming) {
        this.streaming.set(streaming);
    }

    public void withPipes(final Pipes pipes) {
        this.pipes.set(pipes);
    }

    public void withJobXml(final String jobXml) {
        this.jobXmls.add(jobXml);
    }

    public void withoutJobXml(final String jobXml) {
        jobXmls.remove(jobXml);
    }

    public void clearJobXmls() {
        jobXmls.clear();
    }

    /**
     * Setting a key to null means deleting it.
     * @param key
     * @param value
     * @return
     */
    public void withConfigProperty(final String key, final String value) {
        ModifyOnce<String> mappedValue = this.configuration.get(key);

        if (mappedValue == null) {
            mappedValue = new ModifyOnce<>(value);
            this.configuration.put(key, mappedValue);
        }

        mappedValue.set(value);
    }

    public void withConfigClass(final String configClass) {
        this.configClass.set(configClass);
    }

    public void withFile(final String file) {
        this.files.add(file);
    }

    public void withoutFile(final String file) {
        files.remove(file);
    }

    public void clearFiles() {
        files.clear();
    }

    public void withArchive(final String archive) {
        this.archives.add(archive);
    }

    public void withoutArchive(final String archive) {
        archives.remove(archive);
    }

    public void clearArchives() {
        archives.clear();
    }

    public void withDelete(final Delete delete) {
        this.deletes.add(delete);
    }

    public void withoutDelete(final Delete delete) {
        deletes.remove(delete);
    }

    public void clearDeletes() {
        deletes.clear();
    }

    public void withMkdir(final Mkdir mkdir) {
        this.mkdirs.add(mkdir);
    }

    public void withoutMkdir(final Mkdir mkdir) {
        mkdirs.remove(mkdir);
    }

    public void clearMkdirs() {
        mkdirs.clear();
    }

    public void withMove(final Move move) {
        this.moves.add(move);
    }

    public void withoutMove(final Move move) {
        moves.remove(move);
    }

    public void clearMoves() {
        moves.clear();
    }

    public void withChmod(final Chmod chmod) {
        this.chmods.add(chmod);
    }

    public void withoutChmod(final Chmod chmod) {
        chmods.remove(chmod);
    }

    public void clearChmods() {
        chmods.clear();
    }

    public void withTouchz(final Touchz touchz) {
        this.touchzs.add(touchz);
    }

    public void withoutTouchz(final Touchz touchz) {
        touchzs.remove(touchz);
    }

    public void clearTouchzs() {
        touchzs.clear();
    }

    public void withChgrp(final Chgrp chgrp) {
        this.chgrps.add(chgrp);
    }

    public void withoutChgrp(final Chgrp chgrp) {
        chgrps.remove(chgrp);
    }

    public void clearChgrps() {
        chgrps.clear();
    }

    public void withSetrep(final Setrep setrep) {
        this.setreps.add(setrep);
    }

    public void withoutSetrep(final Setrep setrep) {
        setreps.remove(setrep);
    }

    public void clearSetreps() {
        setreps.clear();
    }

    public ActionAttributes build() {
        return new ActionAttributes(
                jobTracker.get(),
                nameNode.get(),
                prepare.get(),
                streaming.get(),
                pipes.get(),
                ImmutableList.copyOf(jobXmls),
                getConfigurationMap(configuration),
                configClass.get(),
                ImmutableList.copyOf(files),
                ImmutableList.copyOf(archives),

                ImmutableList.copyOf(deletes),
                ImmutableList.copyOf(mkdirs),
                ImmutableList.copyOf(moves),
                ImmutableList.copyOf(chmods),
                ImmutableList.copyOf(touchzs),
                ImmutableList.copyOf(chgrps),
                ImmutableList.copyOf(setreps)
        );
    }

    public static Map<String, ModifyOnce<String>> getModifiedOnceMap(final Map<String, String> configurationMap) {
        final Map<String, ModifyOnce<String>> modifyOnceEntries = new LinkedHashMap<>();
        for (final Map.Entry<String, String> keyAndValue : configurationMap.entrySet()) {
            modifyOnceEntries.put(keyAndValue.getKey(), new ModifyOnce<>(keyAndValue.getValue()));
        }

        return modifyOnceEntries;
    }

    public static ImmutableMap<String, String> getConfigurationMap(final Map<String, ModifyOnce<String>> map) {
        final Map<String, String> mutableConfiguration = new LinkedHashMap<>();
        for (final Map.Entry<String, ModifyOnce<String>> modifyOnceEntry : map.entrySet()) {
            if (modifyOnceEntry.getValue().get() != null) {
                mutableConfiguration.put(modifyOnceEntry.getKey(), modifyOnceEntry.getValue().get());
            }
        }

        return ImmutableMap.copyOf(mutableConfiguration);
    }
}
