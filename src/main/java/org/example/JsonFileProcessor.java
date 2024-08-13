package org.example;

// This will be needed if we add object mapper , see comment in field declaration
//import com.fasterxml.jackson.databind.ObjectMapper;

import javax.jms.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

public class JsonFileProcessor {

    private final ConnectionFactory connectionFactory;
    private final String inputDir;
    private final String outputDir;
    // The following will be needed if we want to process the data here
    // Such use cases would be header incrementing or file incrementing
    // private final ObjectMapper objectMapper;

    public JsonFileProcessor(ConnectionFactory connectionFactory, String inputDir, String outputDir) {
        this.connectionFactory = connectionFactory;
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        // see declarations comment
        //this.objectMapper = new ObjectMapper();
    }

    public void processFiles() throws JMSException, IOException {
        File dir = new File(inputDir);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("Input directory does not exist or is not a directory.");
        }

        for (File file : Objects.requireNonNull(dir.listFiles((d, name) -> name.endsWith(".json")))) {
            sendFileToQueue(file);
            moveFileToOutputDir(file);
        }
    }

    private void sendFileToQueue(File file) throws JMSException, IOException {
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("Fileplayer");
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