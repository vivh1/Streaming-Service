/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.streamingserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
Called by StreamingServer insode a thread pool when a new client connects.
runs concurrently in its own worker thread, managing the socket dialog, 
collecting connection parameters, launching FFmpeg and logging structured 
usage statistics when client disconnects.
*/
public class ClientHandler implements Runnable {
    // logger for debugging and tracking server status
    private static final Logger log = LogManager.getLogger(ClientHandler.class);
    // logger for structured usage statistics
    private static final Logger statsLogger = LogManager.getLogger("StatsLogger");
    
    private final Socket clientSocket;
    private final StreamingServer server; 

    // Constructor
    public ClientHandler(Socket socket, StreamingServer server) {
        this.clientSocket = socket;
        this.server = server;
    }

    @Override
    public void run() {
        // track connection start time for stats logging
        long startTime = System.currentTimeMillis(); 
        // tracking variables (for stats)
        int clientSpeedKbps = 0;
        String chosenFile = "unknown";
        String chosenProtocol = "unknown";

        try (
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream())
                ); 
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        ) 
        {
            // Receive speed and format from client
            String speedLine = in.readLine();
            String formatLine = in.readLine();

            if (speedLine == null || formatLine == null) {
                log.warn("Client disconnected before sending speed/format.");
                return;
            }

            // Parse incoming connection attributes
            clientSpeedKbps = Integer.parseInt(speedLine.trim());
            String clientFormat = formatLine.trim().toLowerCase();

            log.info("Client [{}] reported speed: {} Kbps, format preference: {}",
                    clientSocket.getInetAddress(), clientSpeedKbps, clientFormat);

            // Filter files list (method from StreamingServer)
            List<VideoHandler.VideoFile> filtered = server.filterFiles(clientSpeedKbps, clientFormat);
            log.info("Sending {} matching files to client [{}].", filtered.size(), clientSocket.getInetAddress());

            // Send the total number of files
            out.println(filtered.size());

            // Send each matching filename option on its own line
            for (VideoHandler.VideoFile vf : filtered) {
                out.println(vf.toString());
            }

            // Receive client file and protocol selections
            String fileChoiceLine = in.readLine();
            String protocolChoiceLine = in.readLine();

            if (fileChoiceLine == null || protocolChoiceLine == null) {
                log.warn("Client disconnected before sending file/protocol choice.");
                return;
            }

            // Save choices into tracking variables for stats assembly
            chosenFile = fileChoiceLine.trim();
            chosenProtocol = protocolChoiceLine.trim().toUpperCase();
            
            log.info("Client [{}] chose file: {}, protocol: {}", clientSocket.getInetAddress(), chosenFile, chosenProtocol);

            String filePath = server.findFilePath(chosenFile);

            if (filePath == null) {
                log.error("Chosen file not found in available list: {}", chosenFile);
                out.println("ERROR: file not found");
                return;
            }

            out.println("READY");
            log.info("Notified client [{}]: READY to stream.", clientSocket.getInetAddress());

            // Launch FFmpeg external streaming process 
            Process streamingProcess = server.startStreaming(
                    filePath, chosenProtocol, clientSocket.getInetAddress().getHostAddress()
            );     
            
            if (chosenProtocol.equalsIgnoreCase("RTP/UDP")) {
                server.sendSdpFile(out);
            }
            
            // Keep thread alive while vid streams
            if (streamingProcess != null) {
                log.info("Streaming in progress. Holding socket connection open...");
                // block this client worker thread until FFmpeg finishes playing the file
                streamingProcess.waitFor(); 
                log.info("FFmpeg streaming process finished naturally.");
            }

        } catch (NumberFormatException e) {
            log.error("Invalid speed value received from client: {}", e.getMessage());
        } catch (IOException e) {
            log.error("Communication error with client: {}", e.getMessage());
        } catch (InterruptedException e) {
            log.warn("Streaming process thread was interrupted: {}", e.getMessage());
            Thread.currentThread().interrupt(); // Restore the interrupted status flag
        } finally 
        {
            // Calculate total playback session duration in sec
            long endTime = System.currentTimeMillis();
            long playbackDurationSeconds = (endTime - startTime) / 1000;

            // Structured data in format IP;Speed;FileChosen;Protocol;Duration
            statsLogger.info(String.format("%s;%d;%s;%s;%d", 
                    clientSocket.getInetAddress().getHostAddress(),
                    clientSpeedKbps,
                    chosenFile,
                    chosenProtocol,
                    playbackDurationSeconds));

                // Clean up + release network socket resources
                try {
                    clientSocket.close();
                    log.info("Client socket closed for [{}].", clientSocket.getInetAddress());
                } catch (IOException e) {
                    log.error("Error closing client socket: {}", e.getMessage());
                }
        }
    }
}
