package org.apache.oozie.jobs.api.action;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestLauncherBuilder {
    @Test
    public void testAttributesSetOnce() {
        final Launcher launcher = new LauncherBuilder()
                .withMemoryMb(1024)
                .withVCores(2)
                .withQueue("default")
                .withSharelib("default")
                .withViewAcl("default")
                .withModifyAcl("default")
                .build();

        assertEquals(1024, launcher.getMemoryMb());
        assertEquals(2, launcher.getVCores());
        assertEquals("default", launcher.getQueue());
        assertEquals("default", launcher.getSharelib());
        assertEquals("default", launcher.getViewAcl());
        assertEquals("default", launcher.getModifyAcl());
    }
}