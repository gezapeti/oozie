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

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.server.guice.OozieGuiceModule;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ConfigUtils;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 *  Class to start Oozie inside an embedded Jetty server.
 */
public class EmbeddedOozieServer {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedOozieServer.class);
    protected Server server;
    private int httpPort;
    private int httpsPort;
    private final WebAppContext servletContextHandler;
    private final ServletMapper oozieServletMapper;
    private final FilterMapper oozieFilterMapper;
    private JspHandler jspHandler;
    private Services serviceController;
    private SSLServerConnectorFactory sslServerConnectorFactory;
    private Configuration conf;
    private final RewriteHandler oozieRewriteHandler;
    private final ConstraintSecurityHandler constraintSecurityHandler;

    /**
     * Construct Oozie server
     * @param server jetty server to be embedded
     * @param jspHandler handler responsible for setting webapp context for JSP
     * @param serviceController controller for Oozie services; must be already initialized
     * @param sslServerConnectorFactory factory to create server connector configured for SSL
     * @param oozieRewriteHandler URL rewriter
     * @param servletContextHandler main web application context handler
     * @param oozieServletMapper maps servlets to URLs
     * @param oozieFilterMapper  maps filters
     * @param constraintSecurityHandler
     */
    @Inject
    public EmbeddedOozieServer(final Server server,
                               final JspHandler jspHandler,
                               final Services serviceController,
                               final SSLServerConnectorFactory sslServerConnectorFactory,
                               final RewriteHandler oozieRewriteHandler,
                               final WebAppContext servletContextHandler,
                               final ServletMapper oozieServletMapper,
                               final FilterMapper oozieFilterMapper,
                               final ConstraintSecurityHandler constraintSecurityHandler)
    {
        this.constraintSecurityHandler = constraintSecurityHandler;
        this.serviceController = Preconditions.checkNotNull(serviceController, "serviceController is null");
        this.jspHandler = Preconditions.checkNotNull(jspHandler, "jspHandler is null");
        this.sslServerConnectorFactory = Preconditions.checkNotNull(sslServerConnectorFactory,
                "sslServerConnectorFactory is null");
        this.server = Preconditions.checkNotNull(server, "server is null");
        this.oozieRewriteHandler = Preconditions.checkNotNull(oozieRewriteHandler, "rewriter is null");
        this.servletContextHandler = Preconditions.checkNotNull(servletContextHandler, "servletContextHandler is null");
        this.oozieServletMapper = Preconditions.checkNotNull(oozieServletMapper, "oozieServletMapper is null");
        this.oozieFilterMapper = Preconditions.checkNotNull(oozieFilterMapper, "oozieFilterMapper is null");
    }

    /**
     * Set up the Oozie server by configuring jetty server settings and starts Oozie services
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws ServiceException
     */
    public void setup() throws URISyntaxException, IOException, ServiceException {
        conf = serviceController.get(ConfigurationService.class).getConf();

        httpPort = getConfigPort(ConfigUtils.OOZIE_HTTP_PORT);

        HttpConfiguration httpConfiguration = new HttpConfigurationWrapper(conf).getDefaultHttpConfiguration();

        ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));
        connector.setPort(httpPort);
        connector.setHost(conf.get(ConfigUtils.OOZIE_HTTP_HOSTNAME));

        HandlerCollection handlerCollection = new HandlerCollection();

        if (isSecured()) {
            httpsPort =  getConfigPort(ConfigUtils.OOZIE_HTTPS_PORT);
            ServerConnector sslConnector = sslServerConnectorFactory.createSecureServerConnector(httpsPort, conf, server);
            server.setConnectors(new Connector[]{connector, sslConnector});
            constraintSecurityHandler.setHandler(servletContextHandler);
            handlerCollection.addHandler(constraintSecurityHandler);
        }
        else {
            server.setConnectors(new Connector[]{connector});
        }

        servletContextHandler.setContextPath("/oozie/");
        oozieServletMapper.mapOozieServlets();
        oozieFilterMapper.addFilters();

        servletContextHandler.setParentLoaderPriority(true);
        jspHandler.setupWebAppContext(servletContextHandler);

        addErrorHandler();

        handlerCollection.addHandler(servletContextHandler);
        handlerCollection.addHandler(oozieRewriteHandler);
        server.setHandler(handlerCollection);
    }

    private void addErrorHandler() {
        ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
        errorHandler.addErrorPage(404, "/404.html");
        errorHandler.addErrorPage(403, "/403.html");
        servletContextHandler.setErrorHandler(errorHandler);
    }

    private int getConfigPort(String confVar) {
        String confHttpPort = conf.get(confVar);
        int port;
        try {
            port = Integer.parseInt(confHttpPort);
        }
        catch (final NumberFormatException nfe) {
            throw new NumberFormatException(String.format("Port number for '%s \"%s\" ('%s') is not an integer.",
                    confVar, confHttpPort, confHttpPort));
        }
        return port;
    }

    private boolean isSecured() {
        String isSSLEnabled = conf.get("oozie.https.enabled");
        LOG.info("Server started with oozie.https.enabled = " + isSSLEnabled);
        return isSSLEnabled != null && Boolean.valueOf(isSSLEnabled);
    }


    public void start() throws Exception {
        server.start();
        LOG.info("Server started.");
    }

    public void join() throws InterruptedException {
        server.join();
    }

    public void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info("Shutting down.");
                serviceController.destroy();
                LOG.info("Oozie services stopped.");
            }
        });
    }

    public static void main(String[] args) throws Exception {
        final Injector guiceInjector = Guice.createInjector(new OozieGuiceModule());

        final EmbeddedOozieServer embeddedOozieServer = guiceInjector.getInstance(EmbeddedOozieServer.class);

        embeddedOozieServer.setup();
        embeddedOozieServer.addShutdownHook();
        try {
            embeddedOozieServer.start();
        } catch (Exception e) {
            LOG.error("Could not start EmbeddedOozieServer!", e);
            System.exit(1);
        }
        embeddedOozieServer.join();
    }
}
