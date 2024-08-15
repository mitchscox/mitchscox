package org.example;

import java.io.IOException;
import java.nio.file.*;
//import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

public class JsonFilePolling {

    private static final String INPUT_DIR = "/home/bugeye2/IdeaProjects/FilePlayer/input";
    private static final String OUTPUT_DIR = "/home/bugeye2/IdeaProjects/FilePlayer/output";

    public static void main(String[] args) {
        Path inputDir = Paths.get(INPUT_DIR);
        Path outputDir = Paths.get(OUTPUT_DIR);

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            inputDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            while (true) {
                WatchKey key = watchService.poll(10, TimeUnit.SECONDS);
                System.out.println("Polling ");
                System.out.println("Key = " +key);
                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();

                        if (kind == StandardWatchEventKinds.OVERFLOW) {
                            continue;
                        }

                        WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path fileName = ev.context();
                        Path filePath = inputDir.resolve(fileName);

                        if (Files.isRegularFile(filePath) && fileName.toString().endsWith(".json")) {

                            processAndMoveFile(filePath, outputDir);
                        }
                    }

                    boolean valid = key.reset();
                    if (!valid) {
                        break;
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void processAndMoveFile(Path filePath, Path outputDir) {
        try {
            // Add your file processing logic here
            System.out.println("Processing file: " + filePath);
            //JsonFileProcessor jsonFileProcessor = new JsonFileProcessor(connectionFactory, inputDir, outputDir);
            // Move the file to the output directory
            Path targetPath = outputDir.resolve(filePath.getFileName());
            Files.move(filePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Moved file to: " + targetPath);

        } catch (IOException e) {
            System.err.println("Error processing file: " + filePath);
            e.printStackTrace();
        }
    }

}