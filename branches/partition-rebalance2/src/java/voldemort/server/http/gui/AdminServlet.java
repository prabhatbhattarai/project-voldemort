/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.http.gui;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.client.admin.AdminClient;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

/**
 * Main servlet for the admin interface
 * 
 * @author jay
 * 
 */
public class AdminServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VoldemortServer server;
    private VelocityEngine velocityEngine;
    private AdminClient client;

    /* For use by servlet container */
    public AdminServlet() {}

    public AdminServlet(VoldemortServer server, VelocityEngine engine, AdminClient client) {
        this.server = Utils.notNull(server);
        this.velocityEngine = Utils.notNull(engine);
        this.client = client;
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.server = (VoldemortServer) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY));
        this.velocityEngine = (VelocityEngine) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY));

    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        Map<String, Object> params = Maps.newHashMap();
        params.put("cluster", server.getCluster());
        params.put("stores", server.getStoreMap());
        params.put("services", server.getServices());
        params.put("adminService", server.getAdminService());
        velocityEngine.render("admin.vm", params, response.getOutputStream());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String action = req.getParameter("action");

        System.out.println("Action done:" + action);

        if(hasParam(req, "steal")) {
            handleStealRequest(req);
        } else if(hasParam(req, "donate")) {
            handleDonateRequest(req);
        }
        resp.sendRedirect(req.getContextPath());
    }

    private void handleStealRequest(HttpServletRequest req) throws ServletException, IOException {
        String storeName = getParam(req, "storeName");
        // if(storeName.equals("all")) {
        // for(String storeKey: server.getStoreMap().keySet()) {
        // client.stealPartitionsFromCluster(server.getIdentityNode().getId(),
        // storeKey);
        // }
        // } else {
        // client.stealPartitionsFromCluster(server.getIdentityNode().getId(),
        // storeName);
        // }
    }

    private void handleDonateRequest(HttpServletRequest req) throws ServletException, IOException {
        String storeName = getParam(req, "storeName");
        int numPartitions = Integer.parseInt(getParam(req, "numPartitions"));
        boolean deleteNode = false;

        if(numPartitions == -1) {
            deleteNode = true;
            numPartitions = server.getIdentityNode().getNumberOfPartitions();
        }

        // if(storeName.equals("all")) {
        // for(String storeKey: server.getStoreMap().keySet()) {
        // client.donatePartitionsToCluster(server.getIdentityNode().getId(),
        // storeName,
        // numPartitions,
        // deleteNode);
        // }
        // } else {
        // client.donatePartitionsToCluster(server.getIdentityNode().getId(),
        // storeName,
        // numPartitions,
        // deleteNode);
        // }
    }

    public boolean hasParam(HttpServletRequest request, String param) {
        return request.getParameter(param) != null;
    }

    public String getParam(HttpServletRequest request, String name) throws ServletException {
        String p = request.getParameter(name);
        if(p == null || p.equals(""))
            throw new ServletException("Missing required parameter '" + name + "'.");
        else
            return p;
    }
}
