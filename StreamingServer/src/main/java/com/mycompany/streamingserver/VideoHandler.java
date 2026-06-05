/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.streamingserver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
Scans the /videos directory, to find which format / resolution combinations 
are missing for each movie and using FFmpeg creates the missing files.
After processing, it maintains a list of all available VideoFile objects that 
server can send to clients.

Checks for formats: avi, mp4, mkv
           resolutions: 240p, 360p, 480p, 720p, 1080p

File names: MovieName-Resolution.format
Example: Sky-1080p.mp4
*/
public class VideoHandler {

    // Logger instance
    static Logger log = LogManager.getLogger(VideoHandler.class);

    // Supported formats and resolutions
    private static final String[] FORMATS = {"avi", "mp4", "mkv"};
    private static final String[] RESOLUTIONS = {"240p", "360p", "480p", "720p", "1080p"};

    // Actual pixel heights, used for conversion 
    private static final int[] HEIGHTS = {240, 360, 480, 720, 1080};

    private final String videosDir;
    private final FFmpeg ffmpeg;
    private final FFprobe ffprobe;

    // The full list of available video files after scanning n transforming
    private final List<VideoFile> availableFiles = new ArrayList<>();

    // inner class that stores the metadata for one specific version of a video 
    // one combination of name, resolution, format
    // will be used to build the list to send to clients

    public static class VideoFile {

        public final String name;
        public final String resolution;
        public final String format;
        public final String path; // full path on disk

        public VideoFile(String name, String resolution, String format, String path) {
            this.name = name;
            this.resolution = resolution;
            this.format = format;
            this.path = path;
        }

        @Override
        public String toString() {
            return name + "-" + resolution + "." + format;
        }
    }

    // Constructor 
    public VideoHandler(String videosDir) throws IOException {
        this.videosDir = videosDir;
        log.info("Initialising FFmpeg...");
        this.ffmpeg = new FFmpeg("C:\\ffmpeg\\bin\\ffmpeg.exe");
        this.ffprobe = new FFprobe("C:\\ffmpeg\\bin\\ffprobe.exe");
        log.info("FFmpeg initialised successfully.");
    }

    // Main 
    /*
    Scans /videos directory and finds all unique movie names and the highest 
    and resolution available for each.
    Chcks for evry movie if every combination exists, if not creates them.
    */
    public void processVideos() {
        File dir = new File(videosDir);

        // check if /videos exists
        if (!dir.exists() || !dir.isDirectory()) {
            log.error("Videos directory not found: {}", videosDir);
            return;
        }

        // collect all movie names and their highest available resolution
        List<String> movieNames = new ArrayList<>();
        List<String> maxResolution = new ArrayList<>();

        // check if /videos is empty
        File[] files = dir.listFiles();
        if (files == null) {
            log.warn("Videos directory is empty.");
            return;
        }
        // if not empty
        
        // scan all files to find movie names and their max resolutions
        for (File file : files) {
            String filename = file.getName();

            // skip files that are not named MovieName-Resolution.format
            if (!filename.contains("-") || !filename.contains(".")) {
                continue;
            }

            // split on - to separate movie name from resolution and format
            String[] parts = filename.split("-"); //name 1080p.mp4
            if (parts.length < 2) {
                continue;
            }

            String movieName = parts[0];
            String resAndFormat = parts[1];
            
            // split on . to separate resolution from format
            String[] resFormatParts = resAndFormat.split("\\."); // 1080p mp4
            if (resFormatParts.length < 2) {
                continue;
            }

            // skip any file whose resolution label is not one of the supported
            String resolution = resFormatParts[0];

            if (!isValidResolution(resolution)) {
                continue;
            }

            // if seen first time add to list movie name max resolution
            if (!movieNames.contains(movieName)) {
                movieNames.add(movieName);
                maxResolution.add(resolution);
                log.info("Found movie: {} at resolution {}", movieName, resolution);
            } else {
                // update max resolution if this file is higher
                int idx = movieNames.indexOf(movieName);
                
                if (resolutionIndex(resolution) > resolutionIndex(maxResolution.get(idx))) {
                    maxResolution.set(idx, resolution);
                    log.debug("Updated max resolution for {} to {}", movieName, resolution);
                }
            }
        }

        // for every movie check if all formats and resolutions up to max exist
        for (int i = 0; i < movieNames.size(); i++) {
            String movieName = movieNames.get(i);
            // array max index for highest resolution of movie
            int maxIdx = resolutionIndex(maxResolution.get(i));

            log.info("Processing movie: {} (max resolution: {})", movieName, maxResolution.get(i));

            for (int r = 0; r <= maxIdx; r++) {
                for (String format : FORMATS) {
                    String targetFilename = movieName + "-" + RESOLUTIONS[r] + "." + format;
                    File targetFile = new File(videosDir + targetFilename);

                    // if format exists, add it to the list of movies
                    if (targetFile.exists()) {
                        log.debug("Already exists, skipping: {}", targetFilename);
                        availableFiles.add(new VideoFile(movieName, RESOLUTIONS[r], format, targetFile.getAbsolutePath()));
                    } else {
                        // if it doesnt exist
                        // find a source file for this movie to transform
                        String sourceFile = findSourceFile(dir, movieName);
                        
                        if (sourceFile == null) {
                            log.error("No source file found for movie: {}", movieName);
                            continue;
                        }
                        log.info("Transforming: {} -> {}", sourceFile, targetFilename);
                        boolean success = transform(sourceFile, targetFile.getAbsolutePath(), HEIGHTS[r]);
                        
                        if (success) {
                            availableFiles.add(new VideoFile(movieName, RESOLUTIONS[r], format, targetFile.getAbsolutePath()));
                            log.info("Transformed successfully: {}", targetFilename);
                        } else {
                            log.error("Transforming failed for: {}", targetFilename);
                        }
                    }
                }
            }
        }

        log.info("Video processing complete. Total available files: {}", availableFiles.size());
    }

    /*
    uses the FFmpeg wrapper to convert a source video file  into a new file 
    with different resolution or format
    */
    private boolean transform(String inputPath, String outputPath, int targetHeight) {
        try {
            FFmpegBuilder builder = new FFmpegBuilder()
                    .setInput(inputPath)
                    .overrideOutputFiles(true) // overwrite output if it already partially exists
                    .addOutput(outputPath)
                    .setVideoFilter("scale=-2:" + targetHeight) // resize but keep aspect ratio
                    .setVideoCodec("libx264")
                    .setAudioCodec("aac")
                    .done();

            FFmpegExecutor executor = new FFmpegExecutor(ffmpeg, ffprobe);
            executor.createJob(builder).run();
            return true;
        } catch (Exception e) {
            log.error("FFmpeg error during transforming file: {}", e.getMessage());
            return false;
        }
    }

    // finds any existing file for a given movie name
    private String findSourceFile(File dir, String movieName) {
        File[] files = dir.listFiles();
        if (files == null) {
            return null;
        }
        for (File f : files) {
            // any file starting with targetname- belongs to this movie
            if (f.getName().startsWith(movieName + "-")) {
                return f.getAbsolutePath();
            }
        }
        return null;
    }

    // checks if a resolution string is one of the supported values of RESOLUTIONS array
    private boolean isValidResolution(String res) {
        for (String r : RESOLUTIONS) {
            if (r.equals(res)) {
                return true;
            }
        }
        return false;
    }

    // returns the position of a resolution label in RESOLUTIONS array.
    // used to compare resolutions -> a higher index means a higher resolution

    private int resolutionIndex(String res) {
        for (int i = 0; i < RESOLUTIONS.length; i++) {
            if (RESOLUTIONS[i].equals(res)) {
                return i;
            }
        }
        return -1;
    }

    public List<VideoFile> getAvailableFiles() {
        return availableFiles;
    }
/*
    // test
    public static void main(String[] args) {
        String videosDir = System.getProperty("user.dir") + "/videos/";
        log.info("Starting VideoHandler test. Videos dir: {}", videosDir);
        try {
            VideoHandler handler = new VideoHandler(videosDir);
            handler.processVideos();

            log.info("=== Available files ===");
            for (VideoFile vf : handler.getAvailableFiles()) {
                log.info("  {}", vf);
            }
        } catch (IOException e) {
            log.error("Failed to initialise VideoHandler: {}", e.getMessage());
        }
    }
*/
}
