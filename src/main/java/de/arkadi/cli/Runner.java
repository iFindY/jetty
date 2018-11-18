
package de.arkadi.cli;

import de.arkadi.server.RestServer;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Runner {
    private static RestServer server;

    public static void main(String[] args) throws Exception {
        server = new RestServer();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Logger.getLogger(Runner.class.getName()).log(Level.INFO, String.format("=============== Shutting down..."));
                server.stop();
                server = null;
                Logger.getLogger(Runner.class.getName()).log(Level.INFO, String.format("=============== Server shutdown complete..."));
            }
        });
    }
}
