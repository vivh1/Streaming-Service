/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.loadbalancer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
Listens on port 7070 and holds the control ports of the available StreamingServer 
instances. 
For each client that connects, it picks the next server in line (round robin), 
confirms it is reachable and sends back that server's control port as a plain text.
The client then connects to that port instead of a fixed 8000.

The balancer only performs one fast handoff per client.
*/
public class LoadBalancer {

    static Logger log = LogManager.getLogger(LoadBalancer.class);

    // the port the balancer itself listens on
    private static final int BALANCER_PORT = 7070;

    // control ports of the StreamingServer instances to distribute clients across
    private static final int[] serverPorts = {8000, 8010, 8020, 8030, 8040};

    // points to the next server to hand out (advances on every client)
    private static int nextServerIndex = 0;

    public static void main(String[] args) {
        log.info("=== Streaming Load Balancer Starting ===");
        log.info("Listening for clients on port {}", BALANCER_PORT);

        try (ServerSocket balancerSocket = new ServerSocket(BALANCER_PORT)) {
            while (true) {
                Socket clientSocket = balancerSocket.accept();
                log.info("Balancer received a client from: {}", clientSocket.getInetAddress());
                assignServer(clientSocket);
            }
        } catch (IOException e) {
            log.error("Load balancer error: {}", e.getMessage());
        }
    }

    /*
    Picks a server for one client using round robin and replies with its port.
    Starting from current, it tries each server once, wrapping around circularly, 
    until it finds one that is actually listening. If a server is down skip it. 
    If none respond replie with -1.
    */
    private static void assignServer(Socket clientSocket) {
        try (
            Socket socket = clientSocket;
            // plain text so client BufferedReader.readLine() can read it
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            int triedCount = 0;
            // stays -1 if no server is reachable
            int chosenPort = -1;

            while (triedCount < serverPorts.length) {
                int candidatePort = serverPorts[nextServerIndex];
                nextServerIndex = (nextServerIndex + 1) % serverPorts.length;

                // confirm the server is up before handing it out
                try (Socket probe = new Socket()) {
                    probe.connect(new InetSocketAddress("localhost", candidatePort), 500); // 0.5s timeout
                    chosenPort = candidatePort;
                    break;
                } catch (IOException e) {
                    // server not running -> skip it and keep looking 
                    log.warn("Server on port {} not reachable, skipping.", candidatePort);
                }
                triedCount++;
            }

            if (chosenPort == -1) {
                log.error("No available servers for client: {}", clientSocket.getInetAddress());
            } else {
                log.info("Assigned client {} to server port {}", clientSocket.getInetAddress(), chosenPort);
            }

            // send chosen control port back in single text line or -1 (none available)
            out.println(chosenPort);

        } catch (IOException e) {
            log.error("Error while assigning server to client: {}", e.getMessage());
        }
    }
}