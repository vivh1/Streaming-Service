/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package com.mycompany.streamingclient;

import fr.bmartel.speedtest.SpeedTestReport;
import fr.bmartel.speedtest.SpeedTestSocket;
import fr.bmartel.speedtest.inter.ISpeedTestListener;
import fr.bmartel.speedtest.model.SpeedTestError;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/*
Asks the load balancer (port 7070) which streaming server to connect to.
Runs a 5 second download speed test to measure the connection in Kbps.
Asks user to choose a preferred video format (avi, mp4, mkv).
Connects to the assigned StreamingServer over an encrypted SSL socket and sends
the speed and format.
Receives filtered file list from server and displays it.
Asks user to choose a file and optionally streaming protocol.
Sends the choice to the server and waits for the READY signal.
Launches FFplay with ProcessBuilder to receive and play the stream.
 */
public class StreamingClient {

    static Logger log = LogManager.getLogger(StreamingClient.class);

    // Server address and control port
    private static final String SERVER_HOST = "localhost"; 
    // private static final int SERVER_PORT = 8000;
    
    // client first asks the balancer which server /control port to use
    private static final String BALANCER_HOST = "localhost";
    private static final int BALANCER_PORT = 7070;
    
    // SSL/TLS
    // The same streaming.jks file is used as client truststore
    // (it trusts the certificate the server presents)
    private static final String TRUSTSTORE_FILE = "streaming.jks";
    private static final String TRUSTSTORE_PASSWORD = "streaming";

    // Default port FFplay listens on if the server does not send one
    private static final int STREAM_PORT = 9000;

    // The URL used for the download speed test
    private static final String SPEED_TEST_URL
            = "http://speedtest.tele2.net/10MB.zip";

    // Scanner for reading user input from the console
    private final Scanner scanner = new Scanner(System.in);

    public void run() {
        // GUI
        // first window (speed test progress and format choice)
        Waiting waiting = new Waiting();
        waiting.setVisible(true);

        // measure the connection speed
        int speedKbps = measureSpeed(waiting);
        log.info("Measured download speed: {} Kbps", speedKbps);
        
        // show the result and enable the Continue button
        // then wait for user to pick a format and press continue
        waiting.setSpeed(speedKbps);
        String format = waiting.awaitFormatChoice();
        waiting.dispose();

        // ask user to pick a format
        // String format = askFormat();
        log.info("User selected format: {}", format);

        // ask load balancer which server to connect to
        int serverPort = askBalancerForPort();
        if (serverPort == -1) {
            log.error("No streaming server available. Exiting.");
            javax.swing.JOptionPane.showMessageDialog(null,
                "No streaming server is currently available.",
                "Streaming Client", javax.swing.JOptionPane.WARNING_MESSAGE);
            return;
        }
        
        // connect to server and handle communication
        connectAndStream(speedKbps, format, serverPort);
    }
    
    
    private int askBalancerForPort() {
        log.info("Contacting load balancer at {}:{}", BALANCER_HOST, BALANCER_PORT);
        //ask the load balancer which streaming server to use
        try (
            Socket balancerSocket = new Socket(BALANCER_HOST, BALANCER_PORT);
            BufferedReader balancerIn = new BufferedReader(
                    new InputStreamReader(balancerSocket.getInputStream()));
        ) {
            // balancer sends the assigned port or -1
            String portLine = balancerIn.readLine();
            // reeturn that port or -1 on failure
            if (portLine == null) {
                log.error("Load balancer closed the connection without replying.");
                return -1;
            }
 
            int assignedPort = Integer.parseInt(portLine.trim());
            log.info("Load balancer assigned server port: {}", assignedPort);
            return assignedPort;
 
        } catch (IOException e) {
            log.error("Could not reach load balancer: {}", e.getMessage());
            return -1;
        } catch (NumberFormatException e) {
            log.error("Load balancer sent an invalid port value: {}", e.getMessage());
            return -1;
        }
    }

    // 5 second download test using JSpeedTest and returns measured download speed 
    // If speed test fails -> value of 1000 Kbps is returned and application can continue
    // Waiting GUI window is updated with the download progress
    private int measureSpeed(Waiting waiting) {
        log.info("Starting 5 second download speed test...");

        // using an array so it can be set from inside the anonymous listener class 
        final int[] resultKbps = {1000};

        // CountDownLatch with count 1 = call countDown() once the test ends
        // which unblocks the await() call below
        CountDownLatch latch = new CountDownLatch(1);

        SpeedTestSocket speedTestSocket = new SpeedTestSocket();

        speedTestSocket.addSpeedTestListener(new ISpeedTestListener() {

            @Override
            public void onCompletion(SpeedTestReport report) {
                // Convert from bits per second to Kbps
                double bps = report.getTransferRateBit().doubleValue();
                resultKbps[0] = (int) (bps / 1000);
                log.info("Speed test complete: {} Kbps", resultKbps[0]);
                latch.countDown();
            }

            @Override
            public void onError(SpeedTestError error, String message) {
                log.warn("Speed test error: {} - using fallback speed of 1000 Kbps", message);
                latch.countDown();
            }

            @Override
            public void onProgress(float percent, SpeedTestReport report) {
                // Log progress every time JSpeedTest reports an interim result
                double bps = report.getTransferRateBit().doubleValue();
                log.debug("Speed test progress: {}% - current speed: {} Kbps",
                        (int) percent, (int) (bps / 1000));
                // update the GUI progress bar
                if (waiting != null) {
                    waiting.setProgress((int) percent);
                }
            }
        });

        // Start the download test
        speedTestSocket.startDownload(SPEED_TEST_URL);

        try {
            // Block here until test completes or errors
            latch.await();
        } catch (InterruptedException e) {
            log.warn("Speed test interrupted - using fallback speed of 1000 Kbps");
            Thread.currentThread().interrupt();
        }
        
        return resultKbps[0];
    }
    
    /*
    // asks the user to type preferred video format (avi, mp4, mkv are accepted) 
    // repeats until valid format is entered
    private String askFormat() {
        String format = "";
        while (true) {
            System.out.println("Please choose a video format (avi / mp4 / mkv):");
            format = scanner.nextLine().trim().toLowerCase();
            if (format.equals("avi") || format.equals("mp4") || format.equals("mkv")) {
                break;
            }
            System.out.println("Invalid format. Please enter avi, mp4, or mkv.");
        }
        return format;
    }
    */
    
    // Opens the socket connection to the server and handles sending speed+format, 
    // receiving the file list, asking the user to choose, sending the choice, starting FFmpeg
    private void connectAndStream(int speedKbps, String format, int serverPort) {
        log.info("Connecting to server at {}:{}", SERVER_HOST, serverPort);

        // point the JVM at the truststore so client trusts server cert
        System.setProperty("javax.net.ssl.trustStore", TRUSTSTORE_FILE);
        System.setProperty("javax.net.ssl.trustStorePassword", TRUSTSTORE_PASSWORD);
 
        SSLSocketFactory sslFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        
        try (
            SSLSocket socket = (SSLSocket) sslFactory.createSocket(SERVER_HOST, serverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true); 
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));) 
        {
            log.info("Connected to server - SSL.");

            // Send speed and format to the server
            // server expects two lines: speed in Kbps then format
            out.println(speedKbps);
            out.println(format);
            log.info("Sent speed ({} Kbps) and format ({}) to server.", speedKbps, format);

            // Receive the filtered file list from the server
            // The server first sends the number of files, then one filename per line
            String countLine = in.readLine();
            if (countLine == null) {
                log.error("Server closed connection unexpectedly.");
                return;
            }

            int fileCount = Integer.parseInt(countLine.trim());
            log.info("Server sent {} matching files.", fileCount);

            if (fileCount == 0) {
                log.warn("Server returned 0 files for speed {} Kbps and format {}.", speedKbps, format);
                javax.swing.JOptionPane.showMessageDialog(null,
                    "No files available for your connection speed and format.",
                    "Streaming Client", javax.swing.JOptionPane.INFORMATION_MESSAGE);
                return;
            }

            // Read each filename into a list and display it to the user
            List<String> fileList = new ArrayList<>();
            // System.out.println("\nAvailable files:");
            for (int i = 0; i < fileCount; i++) {
                String filename = in.readLine();
                fileList.add(filename);
                // System.out.println("  " + (i + 1) + ". " + filename);
            }

            // Ask user to choose a file
            //String chosenFile = askFileChoice(fileList);
            //log.info("User chose file: {}", chosenFile);
            
            // GUI second window (file list and protocol selection)
            Selection selection = new Selection(speedKbps, fileList);
            selection.setVisible(true);
            selection.awaitChoice(); // blocks until the user presses Stream
            String chosenFile = selection.getChosenFile();
            String chosenProtocol = selection.getChosenProtocol();
            selection.dispose();
            log.info("User chose file: {}", chosenFile);

            // Ask user to choose a protocol
            // The protocol can be chosen manually or assigned automatically
            // based on the resolution of the chosen file
            // String chosenProtocol = askProtocol(chosenFile);
            // log.info("Using protocol: {}", chosenProtocol);
            
            // If user left protocol on Auto, choose it from file's resolution
            if (chosenProtocol.equalsIgnoreCase("Auto")) {
                chosenProtocol = askProtocol(chosenFile);
            }
            log.info("Using protocol: {}", chosenProtocol);

            // Send file and protocol choice to the server
            out.println(chosenFile);
            out.println(chosenProtocol);
            log.info("Sent file choice and protocol to server.");

            // Wait for server READY
            String serverResponse = in.readLine();
            if (serverResponse == null || serverResponse.startsWith("ERROR")) {
                log.error("Server error: {}", serverResponse);
                javax.swing.JOptionPane.showMessageDialog(null,
                        "Server error: " + serverResponse,
                        "Streaming Client", javax.swing.JOptionPane.ERROR_MESSAGE);
                return;
            }

            if (serverResponse.startsWith("READY")) {
                int streamPort = STREAM_PORT;
                
                // pull out the port number the server allocated for this client
                String[] parts = serverResponse.split("\\s+");
                
                if (parts.length >= 2) {
                    try { 
                        streamPort = Integer.parseInt(parts[1].trim()); 
                    } catch (NumberFormatException ignored) { 
                        // keep the default STREAM_PORT if parsing fails
                    }
                }

                log.info("Server is ready. Stream port: {}", streamPort);
                
                // GUI playback status window
                Status status = new Status(chosenFile, chosenProtocol);
                status.setVisible(true);

                Process player;
                // Each protocol needs a different startup order
                if (chosenProtocol.equalsIgnoreCase("RTP/UDP")) {
                    // read the SDP first then start FFplay on it
                    receiveSdpFile(in);
                    player = startReceiving(chosenProtocol, streamPort);
                } else if (chosenProtocol.equalsIgnoreCase("UDP")) {
                    // listen first then tell the server GO
                    player = startReceiving(chosenProtocol, streamPort); 
                    out.println("GO"); 
                } else {
                    // connect to ffmpeg
                    player = startReceiving(chosenProtocol, streamPort);
                }
                
                // stay alive while playing
                if (player != null) {
                    player.waitFor();                                 
                    log.info("Playback finished.");
                }
                
                status.setFinished();
                status.dispose();
            }
        } catch (IOException e) {
            log.error("Connection error: {}", e.getMessage());
        } catch (InterruptedException e) {
            log.warn("Interrupted while streaming: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /*
    // displays the file list and asks user to pick one by entering its number. 
    // Repeats until a valid number is entered.
    private String askFileChoice(List<String> fileList) {
        while (true) {
            System.out.println("\nEnter the number of the file you want to watch:");
            String input = scanner.nextLine().trim();
            try {
                int choice = Integer.parseInt(input);
                if (choice >= 1 && choice <= fileList.size()) {
                    return fileList.get(choice - 1);
                }
            } catch (NumberFormatException ignored) {
            }
            System.out.println("Invalid choice. Enter a number between 1 and " + fileList.size());
        }
    }
    */
    
    /* 
    asks user to manually choose streaming protocol or press Enter to let 
    client choose automatically based on resolution of selected file
    
    240p        -> TCP
    360p,  480p -> UDP
    720p, 1080p -> RTP/UDP
     */
    private String askProtocol(String chosenFile) {
        /* System.out.println("\nChoose a streaming protocol (TCP / UDP / RTP/UDP)");
        System.out.println("Or press Enter to choose automatically based on resolution:");
        String input = scanner.nextLine().trim().toUpperCase();

        // If user typed valid protocol
        if (input.equals("TCP") || input.equals("UDP") || input.equals("RTP/UDP")) {
            return input;
        }
        */
        // else determine from resolution in filename
        String resolution = "";
        try {
            // extract the resolution by splitting on - and then on .
            String[] parts = chosenFile.split("-");
            String resPart = parts[1].split("\\.")[0];  // e.g. "480p"
            resolution = resPart;
        } catch (Exception e) {
            log.warn("Could not parse resolution from filename: {}", chosenFile);
        }

        switch (resolution) {
            case "240p":
                log.info("Auto-selected protocol: TCP (for 240p)");
                return "TCP";
            case "360p":
            case "480p":
                log.info("Auto-selected protocol: UDP (for {})", resolution);
                return "UDP";
            case "720p":
            case "1080p":
                log.info("Auto-selected protocol: RTP/UDP (for {})", resolution);
                return "RTP/UDP";
            default:
                log.warn("Unknown resolution '{}', defaulting to UDP", resolution);
                return "UDP";
        }
    }

    // launches FFplay as a stream receiver using ProcessBuilder
    // FF[lay connects to the stream the server started and plays it back
    private Process startReceiving(String protocol, int streamPort) {
    log.info("Starting FFplay receiver for {} on port {}", protocol, streamPort);

        try {
            List<String> command = new ArrayList<>();
            command.add("C:\\ffmpeg\\bin\\ffplay.exe");

            // close the window automatically when the stream ends only for TCP
            // command.add("-autoexit"); 
            
            // Build input URL based on the chosen protocol
            switch (protocol.toUpperCase()) {
                case "TCP":
                    // ffplay connects out to ffmpeg, who is listening
                    // destination is the reciever
                    command.add("tcp://localhost:" + streamPort);
                    break;
                case "UDP":
                    // ffplay is the reciever, it binds the port and waits
                    //  0.0.0.0 = any interface
                    command.add("udp://0.0.0.0:" + streamPort + "?buffer_size=1048576&listen=1");
                    break;
                case "RTP/UDP":
                    // ffplay reads the SDP file (which already contains the port
                    command.add("-protocol_whitelist");
                    command.add("file,rtp,udp");
                    command.add("-reorder_queue_size"); 
                    command.add("16");   // jitter buffer
                    command.add("-max_delay");
                    command.add("500000");
                    command.add("-i");
                    command.add(System.getProperty("user.dir") + "/video.sdp");
                    break;
                default:
                    // UDP 
                    command.add("udp://0.0.0.0:" + streamPort + "?buffer_size=1048576&listen=1");
            }

            log.info("FFplay receive command: {}", String.join(" ", command));

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.inheritIO();  // lets FFplay use the system console and open its window
            Process process = pb.start();

            log.info("FFplay receiver process started.");
            return process;
        } catch (IOException e) {
            log.error("Failed to start FFplay receiver: {}", e.getMessage());
            return null;
        }
    }

    // reads the SDP file contents sent by the server and writes them to a 
    // local video.sdp file that FFplay will use
    private void receiveSdpFile(BufferedReader in) {
        String sdpPath = System.getProperty("user.dir") + "/video.sdp";
        log.info("Receiving SDP file from server, saving to: {}", sdpPath);

        try (PrintWriter sdpWriter = new PrintWriter(new FileWriter(sdpPath))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.equals("SDP_END")) {
                    break;
                }
                sdpWriter.println(line);
            }
            log.info("SDP file saved successfully.");
        } catch (IOException e) {
            log.error("Error writing SDP file: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        log.info("=== Streaming Client Starting ===");
        new StreamingClient().run();
    }
}
