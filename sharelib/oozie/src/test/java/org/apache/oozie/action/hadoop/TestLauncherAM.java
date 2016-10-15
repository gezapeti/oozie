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
package org.apache.oozie.action.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.JAVA_EXCEPTION;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.JAVA_EXCEPTION_MESSAGE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.LAUNCHER_ERROR_CODE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.LAUNCHER_EXCEPTION;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.SECURITY_EXCEPTION;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.SECURITY_EXCEPTION_MESSAGE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.THROWABLE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.THROWABLE_MESSAGE;

import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_EXTERNAL_CHILD_IDS;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_STATS;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_OUTPUT_PROPS;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_NEW_ID;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTIONOUTPUTTYPE_EXT_CHILD_ID;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTIONOUTPUTTYPE_ID_SWAP;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTIONOUTPUTTYPE_OUTPUT;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTIONOUTPUTTYPE_STATS;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.oozie.action.hadoop.AMRMCallBackHandler;
import org.apache.oozie.action.hadoop.AMRMClientAsyncFactory;
import org.apache.oozie.action.hadoop.HdfsOperations;
import org.apache.oozie.action.hadoop.LauncherAM;
import org.apache.oozie.action.hadoop.LauncherAM.LauncherSecurityManager;
import org.apache.oozie.action.hadoop.LauncherAM.OozieActionResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class TestLauncherAM {
    private static final String ACTIONDATA_ERROR_PROPERTIES = "error.properties";
    private static final String ACTIONDATA_FINAL_STATUS_PROPERTY = "final.status";
    private static final String ERROR_CODE_PROPERTY = "error.code";
    private static final String EXCEPTION_STACKTRACE_PROPERTY = "exception.stacktrace";
    private static final String EXCEPTION_MESSAGE_PROPERTY = "exception.message";
    private static final String ERROR_REASON_PROPERTY = "error.reason";

    private static final String EMPTY_STRING = "";
    private static final String EXIT_CODE_1 = "1";
    private static final String EXIT_CODE_0 = "0";
    private static final String DUMMY_XML = "<dummy>dummyXml</dummy>";

    @Mock
    private UserGroupInformation ugiMock;

    @Mock
    private AMRMClientAsyncFactory amRMClientAsyncFactoryMock;

    @Mock
    private AMRMClientAsync<?> amRmAsyncClientMock;

    @Mock
    private AMRMCallBackHandler callbackHandlerMock;

    @Mock
    private HdfsOperations fsOperationsMock;

    @Mock
    private LocalFsOperations localFsOperationsMock;

    @Mock
    private PrepareActionsHandler prepareHandlerMock;

    @Mock
    private LauncherAMCallbackNotifierFactory launcherCallbackNotifierFactoryMock;

    @Mock
    private LauncherAMCallbackNotifier launcherCallbackNotifierMock;

    @Mock
    private LauncherSecurityManager launcherSecurityManagerMock;

    private Configuration launcherJobConfig = new Configuration();

    private LauncherAM launcherAM;  // unit under test

    @Before
    public void setup() throws IOException {
        launcherAM = new LauncherAM(ugiMock,
                amRMClientAsyncFactoryMock,
                callbackHandlerMock,
                fsOperationsMock,
                localFsOperationsMock,
                prepareHandlerMock,
                launcherCallbackNotifierFactoryMock,
                launcherSecurityManagerMock);

        configureMocks();
    }

    @Test
    public void testMainIsSuccessfullyInvokedWithActionData() throws Exception {
        setupActionOutputContents();

        executeLauncher();

        verifyZeroInteractions(prepareHandlerMock);
        assertSuccessfulExecution(true);
        assertActionOutputData();
    }

    @Test
    public void testMainIsSuccessfullyInvokedWithoutActionData() throws Exception {
        executeLauncher();

        verifyZeroInteractions(prepareHandlerMock);
        assertSuccessfulExecution(false);
        assertNoActionOutputData();
    }

    @Test
    public void testActionHasPrepareXML() throws Exception {
        launcherJobConfig.set(LauncherAM.ACTION_PREPARE_XML, DUMMY_XML);

        executeLauncher();

        verify(prepareHandlerMock).prepareAction(eq(DUMMY_XML), any(Configuration.class));
        assertSuccessfulExecution(false);
    }

    @Test
    public void testActionHasEmptyPrepareXML() throws Exception {
        launcherJobConfig.set(LauncherAM.ACTION_PREPARE_XML, EMPTY_STRING);

        executeLauncher();

        verifyZeroInteractions(prepareHandlerMock);
        assertSuccessfulExecution(false);
        assertNoActionOutputData();
    }

    @Test
    public void testMainIsSuccessfullyInvokedAndAsyncErrorReceived() throws Exception {
        ErrorHolder errorHolder = new ErrorHolder();
        errorHolder.setErrorCode(6);
        errorHolder.setErrorMessage("dummy error");
        errorHolder.setErrorCause(new Exception());
        given(callbackHandlerMock.getError()).willReturn(errorHolder);

        executeLauncher();

        assertFailedExecution(null, "6", "dummy error", true);
    }

    @Test
    public void testMainClassNotFound() throws Exception {
        launcherJobConfig.set(LauncherAM.CONF_OOZIE_ACTION_MAIN_CLASS, "org.apache.non.existing.Klass");

        executeLauncher();

        assertFailedExecution("java.lang.ClassNotFoundException", EXIT_CODE_0, "java.lang.ClassNotFoundException", true);
    }

    @Test
    public void testLauncherJobConfCannotBeLoaded() throws Exception {
        given(localFsOperationsMock.readLauncherConf()).willThrow(new RuntimeException());
        boolean exceptionCaught = false;

        try {
            executeLauncher();
        } catch (Exception e) {
            // expected
            exceptionCaught = true;
        }

        assertTrue(exceptionCaught);
        assertFailedExecution(null, EXIT_CODE_0, "Could not load the Launcher AM configuration file", true);
    }

    @Test
    public void testActionPrepareFails() throws Exception {
        launcherJobConfig.set(LauncherAM.ACTION_PREPARE_XML, DUMMY_XML);
        willThrow(new IOException()).given(prepareHandlerMock).prepareAction(anyString(), any(Configuration.class));

        boolean exceptionCaught = false;

        try {
            executeLauncher();
        } catch (Exception e) {
            // expected
            exceptionCaught = true;
        }

        assertTrue(exceptionCaught);
        assertFailedExecution(null, EXIT_CODE_0, "Prepare execution in the Launcher AM has failed", true);
    }

    @Test
    public void testActionThrowsJavaMainException() throws Exception {
        setupArgsForMainClass(JAVA_EXCEPTION);

        executeLauncher();

        assertFailedExecution(JAVA_EXCEPTION_MESSAGE, EXIT_CODE_0, JAVA_EXCEPTION_MESSAGE, true);
    }

    @Test
    public void testActionThrowsLauncherException() throws Exception {
        setupArgsForMainClass(LAUNCHER_EXCEPTION);

        executeLauncher();

        assertFailedExecution(null, String.valueOf(LAUNCHER_ERROR_CODE), "exit code [" + LAUNCHER_ERROR_CODE + "]", false);
    }

    @Test
    public void testActionThrowsSecurityExceptionWithExitCode0() throws Exception {
        setupArgsForMainClass(SECURITY_EXCEPTION);
        given(launcherSecurityManagerMock.getExitInvoked()).willReturn(true);
        given(launcherSecurityManagerMock.getExitCode()).willReturn(0);

        executeLauncher();

        assertSuccessfulExecution(false);
    }

    @Test
    public void testActionThrowsSecurityExceptionWithExitCode1() throws Exception {
        setupArgsForMainClass(SECURITY_EXCEPTION);
        given(launcherSecurityManagerMock.getExitInvoked()).willReturn(true);
        given(launcherSecurityManagerMock.getExitCode()).willReturn(1);

        executeLauncher();

        assertFailedExecution(null, EXIT_CODE_1, "exit code ["+ EXIT_CODE_1 + "]", false);
    }

    @Test
    public void testActionThrowsSecurityExceptionWithoutSystemExit() throws Exception {
        setupArgsForMainClass(SECURITY_EXCEPTION);
        given(launcherSecurityManagerMock.getExitInvoked()).willReturn(false);

        executeLauncher();

        assertFailedExecution(SECURITY_EXCEPTION_MESSAGE, EXIT_CODE_0, SECURITY_EXCEPTION_MESSAGE, true);
    }

    @Test
    public void testActionThrowsThrowable() throws Exception {
        setupArgsForMainClass(THROWABLE);

        executeLauncher();

        assertFailedExecution(THROWABLE_MESSAGE, EXIT_CODE_0, THROWABLE_MESSAGE, true);
    }

    @Test
    public void testActionThrowsThrowableAndAsyncErrorReceived() throws Exception {
        setupArgsForMainClass(THROWABLE);
        ErrorHolder errorHolder = new ErrorHolder();
        errorHolder.setErrorCode(6);
        errorHolder.setErrorMessage("dummy error");
        errorHolder.setErrorCause(new Exception());
        given(callbackHandlerMock.getError()).willReturn(errorHolder);

        executeLauncher();

        assertFailedExecution(THROWABLE_MESSAGE, EXIT_CODE_0, THROWABLE_MESSAGE, true);  // sync problem overrides async problem
    }

    @Test
    public void testYarnUnregisterFails() throws Exception {
        willThrow(new IOException()).given(amRmAsyncClientMock).unregisterApplicationMaster(any(FinalApplicationStatus.class),
                anyString(), anyString());

        boolean exceptionCaught = false;
        try {
            executeLauncher();
        } catch (Exception e) {
            // expected
            exceptionCaught = true;
        }

        assertTrue(exceptionCaught);

        // TODO: check if this behaviour is correct (url callback: successful, but unregister fails)
        assertSuccessfulExecution(false);
    }

    @Test
    public void testUpdateActionDataFails() throws Exception {
        setupActionOutputContents();
        setupArgsForMainClass(LAUNCHER_EXCEPTION);
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTIONOUTPUTTYPE_EXT_CHILD_ID), anyInt()))
            .willThrow(new IOException());

        executeLauncher();

        Map<String, String> actionData = launcherAM.getActionData();
        assertFalse(actionData.containsKey(ACTION_DATA_EXTERNAL_CHILD_IDS));
        assertFailedExecution(null, String.valueOf(LAUNCHER_ERROR_CODE), "exit code [" + LAUNCHER_ERROR_CODE + "]", false);
    }

    @SuppressWarnings("unchecked")
    // This method configures the mocks for "happy" execution path
    private void configureMocks() throws IOException {
        launcherJobConfig.set(LauncherAM.OOZIE_ACTION_DIR_PATH, "dummy");
        launcherJobConfig.set(LauncherAM.OOZIE_JOB_ID, "dummy");
        launcherJobConfig.set(LauncherAM.OOZIE_ACTION_ID, "dummy");
        launcherJobConfig.set(LauncherAM.CONF_OOZIE_ACTION_MAIN_CLASS, LauncherAMTestMainClass.class.getCanonicalName());

        given(localFsOperationsMock.readLauncherConf()).willReturn(launcherJobConfig);
        given(localFsOperationsMock.fileExists(any(File.class))).willReturn(true);

        willReturn(amRmAsyncClientMock).given(amRMClientAsyncFactoryMock).createAMRMClientAsync(anyInt());
        given(ugiMock.doAs(any(PrivilegedAction.class))).willAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                PrivilegedAction<?> action = (PrivilegedAction<?>) invocation.getArguments()[0];
                return action.run();
            }
        });
        given(launcherCallbackNotifierFactoryMock.createCallbackNotifier(any(Configuration.class))).willReturn(launcherCallbackNotifierMock);
    }

    private void setupActionOutputContents() throws IOException {
        // output files generated by an action
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTIONOUTPUTTYPE_EXT_CHILD_ID), anyInt())).willReturn(ACTIONOUTPUTTYPE_EXT_CHILD_ID);
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTIONOUTPUTTYPE_ID_SWAP), anyInt())).willReturn(ACTIONOUTPUTTYPE_ID_SWAP);
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTIONOUTPUTTYPE_OUTPUT), anyInt())).willReturn(ACTIONOUTPUTTYPE_OUTPUT);
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTIONOUTPUTTYPE_STATS), anyInt())).willReturn(ACTIONOUTPUTTYPE_STATS);
    }

    private void setupArgsForMainClass(final String...  args) {
        launcherJobConfig.set(String.valueOf(LauncherAM.CONF_OOZIE_ACTION_MAIN_ARG_COUNT), String.valueOf(args.length));

        for (int i = 0; i < args.length; i++) {
            launcherJobConfig.set(String.valueOf(LauncherAM.CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i), args[i]);
        }
    }

    private void executeLauncher() throws Exception {
        launcherAM.run();
    }

    @SuppressWarnings("unchecked")
    private void assertSuccessfulExecution(boolean backgroundAction) throws Exception {
        verify(amRmAsyncClientMock).registerApplicationMaster(anyString(), anyInt(), anyString());
        verify(amRmAsyncClientMock).unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, EMPTY_STRING, EMPTY_STRING);
        verify(amRmAsyncClientMock).stop();
        verify(ugiMock, times(2)).doAs(any(PrivilegedAction.class)); // prepare & action main
        verify(fsOperationsMock).uploadActionDataToHDFS(any(Configuration.class), any(Path.class), any(Map.class));
        verify(launcherCallbackNotifierFactoryMock).createCallbackNotifier(any(Configuration.class));
        if (backgroundAction) {
            verify(launcherCallbackNotifierMock).notifyURL(OozieActionResult.RUNNING);
        } else {
            verify(launcherCallbackNotifierMock).notifyURL(OozieActionResult.SUCCEEDED);
        }
        Map<String, String> actionData = launcherAM.getActionData();
        verifyFinalStatus(actionData, backgroundAction ? OozieActionResult.RUNNING.toString() : OozieActionResult.SUCCEEDED.toString());
        verifyNoError(actionData);
    }

    private void assertActionOutputData() {
        Map<String, String> actionData = launcherAM.getActionData();
        String extChildId = actionData.get(ACTION_DATA_EXTERNAL_CHILD_IDS);
        String stats = actionData.get(ACTION_DATA_STATS);
        String output = actionData.get(ACTION_DATA_OUTPUT_PROPS);
        String idSwap = actionData.get(ACTION_DATA_NEW_ID);

        assertEquals(ACTIONOUTPUTTYPE_EXT_CHILD_ID, extChildId);
        assertEquals(ACTIONOUTPUTTYPE_STATS, stats);
        assertEquals(ACTIONOUTPUTTYPE_OUTPUT, output);
        assertEquals(ACTIONOUTPUTTYPE_ID_SWAP, idSwap);
    }

    private void assertNoActionOutputData() {
        Map<String, String> actionData = launcherAM.getActionData();
        String extChildId = actionData.get(ACTION_DATA_EXTERNAL_CHILD_IDS);
        String stats = actionData.get(ACTION_DATA_STATS);
        String output = actionData.get(ACTION_DATA_OUTPUT_PROPS);
        String idSwap = actionData.get(ACTION_DATA_NEW_ID);

        assertNull(extChildId);
        assertNull(stats);
        assertNull(output);
        assertNull(idSwap);
    }

    private void assertFailedExecution(String expectedExceptionMessage, String expectedErrorCode, String expectedErrorReason, boolean hasStackTrace) throws Exception {
        Map<String, String> actionData = launcherAM.getActionData();
        verify(launcherCallbackNotifierFactoryMock).createCallbackNotifier(any(Configuration.class));
        verify(launcherCallbackNotifierMock).notifyURL(OozieActionResult.FAILED);
        verifyFinalStatus(actionData, OozieActionResult.FAILED.toString());

        String fullError = actionData.get(ACTIONDATA_ERROR_PROPERTIES);
        Properties props = new Properties();
        props.load(new StringReader(fullError));

        String errorReason = props.getProperty(ERROR_REASON_PROPERTY);
        if (expectedErrorReason != null) {
            assertNotNull(errorReason);
            assertTrue(errorReason.contains(expectedErrorReason));
        } else {
            assertNull(errorReason);
        }

        String exceptionMessage = props.getProperty(EXCEPTION_MESSAGE_PROPERTY);
        if (expectedExceptionMessage != null) {
            assertNotNull(exceptionMessage);
            assertTrue(exceptionMessage.contains(expectedExceptionMessage));
        } else {
            assertNull(exceptionMessage);
        }

        String stackTrace = props.getProperty(EXCEPTION_STACKTRACE_PROPERTY);
        if (hasStackTrace) {
            assertNotNull(stackTrace);
        } else {
            assertNull(stackTrace);
        }

        String errorCode = props.getProperty(ERROR_CODE_PROPERTY);
        assertEquals(expectedErrorCode, errorCode);
    }

    private void verifyFinalStatus(Map<String, String> actionData, String expectedFinalStatus) {
        String finalStatus = actionData.get(ACTIONDATA_FINAL_STATUS_PROPERTY);
        assertEquals(expectedFinalStatus, finalStatus);
    }

    private void verifyNoError(Map<String, String> actionData) {
        String fullError = actionData.get(ACTIONDATA_ERROR_PROPERTIES);
        assertNull(fullError);
    }
}
