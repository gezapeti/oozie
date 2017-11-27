package org.apache.oozie.jobs.api.action;

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSshActionBuilder extends TestNodeBuilderBaseImpl<SshAction, SshActionBuilder> {
    private static final String NAME = "ssh-name";
    private static final String[] ARGS = {"arg1", "arg2", "arg3"};
    private static final String DEFAULT = "default";

    @Override
    protected SshActionBuilder getBuilderInstance() {
        return SshActionBuilder.create();
    }

    @Override
    protected SshActionBuilder getBuilderInstance(final SshAction action) {
        return SshActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testSeveralArgsAdded() {
        final SshActionBuilder builder = getBuilderInstance();

        for (final String arg : ARGS) {
            builder.withArg(arg);
        }

        final SshAction action = builder.build();

        final List<String> argList = action.getArgs();
        assertEquals(ARGS.length, argList.size());

        for (int i = 0; i < ARGS.length; ++i) {
            assertEquals(ARGS[i], argList.get(i));
        }
    }

    @Test
    public void testRemoveArgs() {
        final SshActionBuilder builder = getBuilderInstance();

        for (final String file : ARGS) {
            builder.withArg(file);
        }

        builder.withoutArg(ARGS[0]);

        final SshAction action = builder.build();

        final List<String> argList = action.getArgs();
        final String[] remainingArgs = Arrays.copyOfRange(ARGS, 1, ARGS.length);
        assertEquals(remainingArgs.length, argList.size());

        for (int i = 0; i < remainingArgs.length; ++i) {
            assertEquals(remainingArgs[i], argList.get(i));
        }
    }

    @Test
    public void testClearArgs() {
        final SshActionBuilder builder = getBuilderInstance();

        for (final String file : ARGS) {
            builder.withArg(file);
        }

        builder.clearArgs();

        final SshAction action = builder.build();

        final List<String> argList = action.getArgs();
        assertEquals(0, argList.size());
    }

    @Test
    public void testFromExistingSshAction() {
        final SshActionBuilder builder = getBuilderInstance();

        builder.withName(NAME)
                .withCommand(DEFAULT)
                .withHost(DEFAULT)
                .withArg(ARGS[0])
                .withArg(ARGS[1])
                .withCaptureOutput(true);

        final SshAction action = builder.build();

        final SshActionBuilder fromExistingBuilder = getBuilderInstance(action);

        final String newName = "fromExisting_" + NAME;
        fromExistingBuilder.withName(newName)
                .withoutArg(ARGS[1])
                .withArg(ARGS[2]);

        final SshAction modifiedAction = fromExistingBuilder.build();

        assertEquals(newName, modifiedAction.getName());

        assertEquals(Arrays.asList(ARGS[0], ARGS[2]), modifiedAction.getArgs());

        assertEquals(action.getCommand(), modifiedAction.getCommand());
        assertEquals(action.getHost(), modifiedAction.getHost());
        assertEquals(action.isCaptureOutput(), modifiedAction.isCaptureOutput());
    }
}