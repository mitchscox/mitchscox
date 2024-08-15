package org.example;

/* This will be needed if we add object mapper , see comment in field declaration
 in this particular class file */

//import com.fasterxml.jackson.databind.ObjectMapper;

import javax.jms.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
//import java.nio.file.attribute.BasicFileAttributes;

public class JsonFileProcessor {

    private final ConnectionFactory connectionFactory;
    private final String inputDir;
    private final String outputDir;
    private static final String INPUT_DIR = "/home/bugeye2/IdeaProjects/FilePlayer/input";
    private static final String OUTPUT_DIR = "/home/bugeye2/IdeaProjects/FilePlayer/output";

    /* The following will be needed if we want to process the data here
     Such use cases would be header incrementing or file incrementing */

    // private final ObjectMapper objectMapper;

    public JsonFileProcessor(ConnectionFactory connectionFactory, String inputDir, String outputDir) {
        this.connectionFactory = connectionFactory;
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        // see declarations comment
        //this.objectMapper = new ObjectMapper();
    }

    public void processFiles() throws JMSException, IOException {
        Path inputDir = Paths.get(INPUT_DIR);
        Path outputDir = Paths.get(OUTPUT_DIR);

       // TODO fix directory existence check
       /* if (!inputDir.exists() || !inputDir.isDirectory()) {
            throw new IllegalArgumentException("Input directory does not exist or is not a directory.");
        }*/

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            // todo check if files already exist
            inputDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            System.out.println("Created the watch service polling for :" +inputDir);
            System.out.println("Files will be placed in : "+outputDir);
            while (true) {
                WatchKey key = watchService.poll(10, TimeUnit.SECONDS);
                System.out.println("Polling event");
                System.out.println("Key = " +key);
                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        System.out.println("Ive in the for loop");
                        if (kind == StandardWatchEventKinds.OVERFLOW) {
                            continue;
                        }

                        WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path fileName = ev.context();
                        Path filePath = inputDir.resolve(fileName);

                        if (Files.isRegularFile(filePath) && fileName.toString().endsWith(".json")) {
                            System.out.println("Im processing a file");
                            //sendFileToQueue(file);
                           // moveFileToOutputDir(file);
                           // processAndMoveFile(filePath, outputDir);
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

    private void sendFileToQueue(File file) throws JMSException, IOException {
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("FilePlayer");
            MessageProducer producer = session.createProducer(queue);

            String jsonContent = new String(Files.readAllBytes(file.toPath()));
            TextMessage message = session.createTextMessage(jsonContent);
            producer.send(message);

            System.out.println("Sent file to queue: " + file.getName());

    }

    private void moveFileToOutputDir(File file) throws IOException {
        Path targetPath = new File(outputDir, file.getName()).toPath();
        Files.move(file.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
        System.out.println("Moved file to output directory: " + file.getName());
    }

    public static void main(String[] args) {
        try {
            ConnectionFactory connectionFactory = JMSConfig.connectionFactory();
            String inputDir = "/home/bugeye2/IdeaProjects/FilePlayer/input";
            String outputDir = "/home/bugeye2/IdeaProjects/FilePlayer/output";

            JsonFileProcessor processor = new JsonFileProcessor(connectionFactory, inputDir, outputDir);
            processor.processFiles();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}