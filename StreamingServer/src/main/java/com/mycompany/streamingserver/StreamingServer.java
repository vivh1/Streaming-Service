/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package com.mycompany.streamingserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
Runs VideoHandler to scan /videos and create any missing files
Opens a ServerSocket on port 8000 and waits for a client to connect.
When client connects  a ClientHandler task is submitted to a thread
pool, so multiple clients can be served at the same time.
Every ClientHander:
Receives the client download speed (Kbps) and chosen format
Finds the available file list and returns matching
Receives client chosen filename and chosen protocol
Starts FFmpeg streaming using ProcessBuilder 

 All communication over the socket uses plain text lines (PrintWriter / BufferedReader).
 */
public class StreamingServer {

    static Logger log = LogManager.getLogger(StreamingServer.class);

    private static final int PORT = 8000;

    // Bitrate thresholds in Kbps
    // A resolution is included in the filtered list only if maximum bitrate
    // is less or equal to the client reported download speed
    private static final String[] RESOLUTIONS = {"240p", "360p", "480p", "720p", "1080p"};
    private static final int[] MAX_BITRATES = {700, 1000, 2000, 4000, 6000};

    private VideoHandler videoHandler;
    
    // manage multiple client threads safely
    private final ExecutorService threadPool = Executors.newFixedThreadPool(10);

    // Each client gets its own block of ports so concurrent FFmpeg processes
    // never collide on the same socket
    //  base -> media / RTP port
    // base + 1 -> RTCP port (only for RTP/UDP)
    // The next client starts 4 ports higher, leaving room for the RTCP pair
    private final AtomicInteger nextStreamPort = new AtomicInteger(9000);
    
    // Constructor 
    // initialises VideoHandler, scans /videos and transforms missing 
    // files before the server starts accepting connections
    public StreamingServer() throws IOException {
        String videosDir = System.getProperty("user.dir") + "/videos/";
        log.info("Initialising VideoHandler, videos directory: {}", videosDir);
        videoHandler = new VideoHandler(videosDir);
        videoHandler.processVideos();
        log.info("VideoHandler ready. Total available files: {}",
                videoHandler.getAvailableFiles().size());
    }

    // opens the ServerSocket and enters the main accept loop
    // every accepted client is wrapped in a ClientHandler task and handed to
    // the thread pool, so the loop is free to accept the next client
    public void start() {
        log.info("Starting server on port {}...", PORT);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            log.info("Server is listening on port {}", PORT);

            // keeps running so the server can serve multiple clients 
            while (true) {
                log.info("Waiting for client connection...");
                Socket clientSocket = serverSocket.accept();
                log.info("Client connected from: {}", clientSocket.getInetAddress());

                // Handle the connected client // handleClient(clientSocket);
                // Instead handleClient() and blocking the loop wrap the 
                // socket handling in a task and submit it to the thread pool
                // Pass this so ClientHandler can access helper methods
                threadPool.execute(new ClientHandler(clientSocket, this));
            }

        } catch (IOException e) {
            log.error("Server error: {}", e.getMessage());
        } finally {
            // Clean thread pool on shutdown
            threadPool.shutdown();
        }
    }
    /*  
    ClientHandler reolaces this
    // manages the full communication sequence with one client
    // reads and writes plain text lines over the socket 
    private void handleClient(Socket clientSocket) {
        try (
            BufferedReader in = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream())
            ); 
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true); 
            // autoFlush=true means every println() is sent immediately
            ) 
        {

            // Receive speed and format from client
            // (client sends speed in Kbps then format)
            String speedLine = in.readLine();
            String formatLine = in.readLine();

            if (speedLine == null || formatLine == null) {
                log.warn("Client disconnected before sending speed/format.");
                return;
            }

            int clientSpeedKbps = Integer.parseInt(speedLine.trim());
            String clientFormat = formatLine.trim().toLowerCase();

            log.info("Client reported speed: {} Kbps, format preference: {}",
                    clientSpeedKbps, clientFormat);

            // Filter the file list and send to client
            List<VideoHandler.VideoFile> filtered = filterFiles(clientSpeedKbps, clientFormat);
            log.info("Sending {} matching files to client.", filtered.size());

            // send the number of files
            out.println(filtered.size());

            // send each filename on its own line
            for (VideoHandler.VideoFile vf : filtered) {
                out.println(vf.toString());
                log.debug("Sent file option: {}", vf);
            }

            // Receive client file and protocol 
            String chosenFile = in.readLine();
            String chosenProtocol = in.readLine();

            if (chosenFile == null || chosenProtocol == null) {
                log.warn("Client disconnected before sending file/protocol choice.");
                return;
            }

            log.info("Client chose file: {}, protocol: {}", chosenFile, chosenProtocol);

            // Start streaming
            // Find the full path for the chosen file
            String filePath = findFilePath(chosenFile);

            if (filePath == null) {
                log.error("Chosen file not found in available list: {}", chosenFile);
                out.println("ERROR: file not found");
                return;
            }

            // Notify the client that streaming is about to start
            out.println("READY");
            log.info("Notified client: READY to stream.");

            // Start the FFmpeg stream (ProcessBuilder implementation)
            startStreaming(filePath, chosenProtocol, clientSocket.getInetAddress().getHostAddress());

            // For RTP/UDP send the generated SDP file to client over the socket
            if (chosenProtocol.equalsIgnoreCase("RTP/UDP")) {
                sendSdpFile(out);
            }

        } catch (NumberFormatException e) {
            log.error("Invalid speed value received from client: {}", e.getMessage());
        } catch (IOException e) {
            log.error("Communication error with client: {}", e.getMessage());
        } finally {
            try {
                clientSocket.close();
                log.info("Client socket closed.");
            } catch (IOException e) {
                log.error("Error closing client socket: {}", e.getMessage());
            }
        }
    }
    */

    /* returns only the files that match the client format preference and 
       are at a resolution supported by the client connection speed.
       240p  -> max 700 Kbps 
       360p  -> max 1000 Kbps 
       480p  -> max 2000 Kbps 
       720p  -> max 4000 Kbps 
       1080p -> max 6000 Kbps
    
       A resolution is included if its max bitrate <= clientSpeedKbps.
     */
    public List<VideoHandler.VideoFile> filterFiles(int clientSpeedKbps, String clientFormat) {
        List<VideoHandler.VideoFile> result = new ArrayList<>();

        for (VideoHandler.VideoFile vf : videoHandler.getAvailableFiles()) {

            // Check format matches what the client requested
            if (!vf.format.equalsIgnoreCase(clientFormat)) {
                continue;
            }

            // Check the resolution is within client bandwidth capacity
            int maxBitrateForResolution = getMaxBitrateForResolution(vf.resolution);
            if (maxBitrateForResolution == -1) {
                continue;
            }

            if (maxBitrateForResolution <= clientSpeedKbps) {
                result.add(vf);
            }
        }

        return result;
    }

    // returns the maximum bitrate in Kbps for given resolution label
    private int getMaxBitrateForResolution(String resolution) {
        for (int i = 0; i < RESOLUTIONS.length; i++) {
            if (RESOLUTIONS[i].equalsIgnoreCase(resolution)) {
                return MAX_BITRATES[i];
            }
        }
        return -1;
    }

    // finds the bitrate of a file from its name
    // reuses getMaxBitrateForResolution so the bitrate table is found in one place 
    // used by ClientHandler when writing the stats line
    // returns -1 if the resolution cannot be parsed
    public int getBitrateForFilename(String filename) {
        try {
            // filename pattern Name-Resolution.format
            String[] parts = filename.split("-");
            String resolution = parts[1].split("\\.")[0];
            return getMaxBitrateForResolution(resolution);
        } catch (Exception e) {
            log.warn("Could not parse bitrate from filename: {}", filename);
            return -1;
        }
    }
    
    // searches the available file list for a file whose toString() matches 
    // chosen filename string sent by client
    public String findFilePath(String chosenFile) {
        for (VideoHandler.VideoFile vf : videoHandler.getAvailableFiles()) {
            if (vf.toString().equalsIgnoreCase(chosenFile)) {
                return vf.path;
            }
        }
        return null;
    }

    // sends new port block for one client and increases the counter for the next
    public int allocateStreamPort() {
        // getAndAdd returns the current value, then increases it by 4
        return nextStreamPort.getAndAdd(4);
    }
 
    // Builds one per client SDP path from its base port (video_9000.sdp)
    // A unique file per client means two concurrent RTP/UDP sessions never
    // overwrite each ones SDP
    public String getSdpPathFor(int basePort) {
        return System.getProperty("user.dir") + "/video_" + basePort + ".sdp";
    }
    
    // launches FFmpeg as a streaming server using ProcessBuilder
    // is called after the client has chosen a file and protocol
    public Process startStreaming(String filePath, String protocol, String clientIP,
            int basePort, String sdpPath) {
        log.info("Starting {} stream for file: {}", protocol, filePath, basePort);

        try {
            List<String> command = new ArrayList<>();
            command.add("C:\\ffmpeg\\bin\\ffmpeg.exe");

            switch (protocol.toUpperCase()) {
                case "TCP":
                    // FFmpeg is the listenerI
                    // binds the port and waits for ffplay to connect
                    command.add("-i");
                    command.add(filePath);
                    command.add("-f");
                    command.add("avi");
                    command.add("tcp://localhost:" + basePort + "?listen");
                    break;
                case "UDP":
                    //FFmpeg is the sender
                    // sends packets at the client
                    command.add("-re");
                    command.add("-i");
                    command.add(filePath);
                    command.add("-f");
                    command.add("avi");
                    command.add("udp://" + clientIP + ":" + basePort);
                    break;
                case "RTP/UDP":
                    // sends at client and writes SDP file
                    command.add("-re");
                    command.add("-i");
                    command.add(filePath);
                    command.add("-an"); // no audio
                    command.add("-c:v");
                    command.add("copy"); // no re-encoding
                    command.add("-f");
                    command.add("rtp");
                    command.add("-sdp_file");
                    command.add(sdpPath);
                    command.add("rtp://" + clientIP + ":" + basePort + 
                            "?rtcpport=" + (basePort + 1));
                    break;
                default:
                    log.warn("Unknown protocol: {}. Defaulting to UDP.", protocol);
                    command.add("-re");
                    command.add("-i");
                    command.add(filePath);
                    command.add("-f");
                    command.add("avi");
                    command.add("udp://" + clientIP + ":" + basePort);
            }

            log.info("FFmpeg stream command: {}", String.join(" ", command));

            ProcessBuilder pb = new ProcessBuilder(command);
            // Merge FFmpeg stderr into stdout to see output in the log
            pb.redirectErrorStream(true);
            Process process = pb.start();

            // Read FFmpeg output in a separate thread so it doesn't block the server
            new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        log.debug("[FFmpeg] {}", line);
                    }
                } catch (IOException e) {
                    log.error("Error reading FFmpeg output: {}", e.getMessage());
                }
            }).start();

            log.info("FFmpeg streaming process started.");
            return process;

        } catch (IOException e) {
            log.error("Failed to start FFmpeg streaming process: {}", e.getMessage());
            return null;
        }
    }

    //creates StreamingServer instance and calls start()
    public static void main(String[] args) {
        log.info("=== Streaming Server Starting ===");
        try {
            StreamingServer server = new StreamingServer();
            server.start();
        } catch (IOException e) {
            log.error("Failed to initialise server: {}", e.getMessage());
        }
    }

    // Reads the SDP file that FFmpeg generated for this client and sends its
    // contents over the socket. 
    // The client writes these lines to its own local video.sdp file.
    public void sendSdpFile(PrintWriter out, String sdpPath) {
        File sdpFile = new File(sdpPath);

        // wait up to 3 seconds for SDP file appear
        int attempts = 0;
        
        while (!sdpFile.exists() && attempts < 30) {
            try {
                Thread.sleep(100);
                attempts++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (!sdpFile.exists()) {
            log.error("SDP file was not created by FFmpeg: {}", sdpPath);
            out.println("SDP_END");
            return;
        }

        try (BufferedReader sdpReader = new BufferedReader(new FileReader(sdpFile))) {
            String line;
            while ((line = sdpReader.readLine()) != null) {
                out.println(line);
            }
            out.println("SDP_END");
            log.info("SDP file sent to client.");
        } catch (IOException e) {
            log.error("Error reading SDP file: {}", e.getMessage());
            out.println("SDP_END");
        }
    }
}
