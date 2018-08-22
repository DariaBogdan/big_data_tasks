package com.epam.bcc.htm;

import py4j.GatewayServer;

// Expose HTMNetworkPool to python world
public class PythonGateway {
    public static void main(String[] args) {
        HTMNetworkPool pool = new HTMNetworkPool();
        GatewayServer server = new GatewayServer(pool);
        server.start();
    }
}