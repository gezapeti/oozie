package org.apache.oozie.jobs.api.mapping;

import org.apache.oozie.jobs.api.action.Launcher;
import org.apache.oozie.jobs.api.generated.workflow.LAUNCHER;
import org.apache.oozie.jobs.api.generated.workflow.ObjectFactory;
import org.dozer.DozerConverter;

public class InlineWorkflowLauncherConverter extends DozerConverter<Launcher, LAUNCHER> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public InlineWorkflowLauncherConverter() {
        super(Launcher.class, LAUNCHER.class);
    }

    @Override
    public LAUNCHER convertTo(final Launcher source, LAUNCHER destination) {
        if (source == null) {
            return null;
        }

        destination = ensureDestination(destination);

        mapAttributes(source, destination);

        return destination;
    }

    private LAUNCHER ensureDestination(final LAUNCHER destination) {
        if (destination == null) {
            return OBJECT_FACTORY.createLAUNCHER();
        }

        return destination;
    }

    private void mapAttributes(final Launcher source, final LAUNCHER destination) {
        if (source == null) {
            return;
        }

        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERMemoryMb(source.getMemoryMb()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERVcores(source.getVCores()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERQueue(source.getQueue()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERSharelib(source.getSharelib()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERViewAcl(source.getViewAcl()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERModifyAcl(source.getModifyAcl()));
    }

    @Override
    public Launcher convertFrom(final LAUNCHER source, final Launcher destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
