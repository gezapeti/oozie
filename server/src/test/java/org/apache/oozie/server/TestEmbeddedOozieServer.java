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

package org.apache.oozie.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 *  Server tests
 */
@RunWith(MockitoJUnitRunner.class)
public class TestEmbeddedOozieServer {
    @Mock private JspHandler mockJspHandler;
    @Mock private Services mockServices;
    @Mock private SslContextFactory mockSSLContextFactory;
    @Mock private SSLServerConnectorFactory mockSSLServerConnectorFactory;
    @Mock private Server mockServer;
    @Mock private ServerConnector mockServerConnector;
    @Mock private ConfigurationService mockConfigService;
    @Mock private Configuration mockConfiguration;
    @Mock private RewriteHandler mockOozieRewriteHandler;
    @Mock private EmbeddedOozieServer embeddedOozieServer;
    @Mock private WebAppContext servletContextHandler;
    @Mock private ServletMapper oozieServletMapper;
    @Mock private FilterMapper oozieFilterMapper;
    @Mock private ConstraintSecurityHandler constraintSecurityHandler;

    @Before public void setUp() {
        embeddedOozieServer = new EmbeddedOozieServer(mockServer, mockJspHandler, mockServices, mockSSLServerConnectorFactory,
                mockOozieRewriteHandler, servletContextHandler, oozieServletMapper, oozieFilterMapper, constraintSecurityHandler);

        doReturn("11000").when(mockConfiguration).get("oozie.http.port");
        doReturn("11443").when(mockConfiguration).get("oozie.https.port");
        doReturn("65536").when(mockConfiguration).get("oozie.http.request.header.size");
        doReturn("65536").when(mockConfiguration).get("oozie.http.response.header.size");
        doReturn("42").when(mockConfiguration).get("oozie.server.threadpool.max.threads");
        doReturn(mockConfiguration).when(mockConfigService).getConf();
        doReturn(mockConfigService).when(mockServices).get(ConfigurationService.class);
    }

    @After public void tearDown() {
        verify(mockServices).get(ConfigurationService.class);

        verifyNoMoreInteractions(
                mockJspHandler,
                mockServices,
                mockServerConnector,
                mockSSLServerConnectorFactory);
    }

    @Test
    public void testServerSetup() throws Exception {
        doReturn("false").when(mockConfiguration).get("oozie.https.enabled");
        embeddedOozieServer.setup();
        verify(mockJspHandler).setupWebAppContext(isA(WebAppContext.class));
    }

    @Test
    public void testSecureServerSetup() throws Exception {
        doReturn("true").when(mockConfiguration).get("oozie.https.enabled");

        ServerConnector mockSecuredServerConnector = new ServerConnector(embeddedOozieServer.server);
        doReturn(mockSecuredServerConnector)
                .when(mockSSLServerConnectorFactory)
                .createSecureServerConnector(anyInt(), any(Configuration.class), any(Server.class));

        embeddedOozieServer.setup();

        verify(mockJspHandler).setupWebAppContext(isA(WebAppContext.class));
        verify(mockSSLServerConnectorFactory).createSecureServerConnector(
                isA(Integer.class), isA(Configuration.class), isA(Server.class));
    }

    @Test(expected=NumberFormatException.class)
    public void numberFormatExceptionThrownWithInvalidHttpPort() throws ServiceException, IOException, URISyntaxException {
        doReturn("INVALID_PORT").when(mockConfiguration).get("oozie.http.port");
        embeddedOozieServer.setup();
    }
}
