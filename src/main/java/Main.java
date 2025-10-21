import dto.KafkaResponse;
import dto.MetadataCache;
import dto.TopicClusterMetadata;
import handlers.ClientHandler;
import processors.ClusterMetadataProcessor;
import processors.RequestProcessor;
import processors.ResponseProcessor;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  static final List<KafkaResponse.ApiVersionDTO> supportedApis = List.of(
          new KafkaResponse.ApiVersionDTO((short)18, (short)0, (short)4),    // ApiVersions
          new KafkaResponse.ApiVersionDTO((short)1, (short)4, (short)16),    // Fetch
          new KafkaResponse.ApiVersionDTO((short)75, (short)0, (short)0)    // DescribeTopicPartitions
  );

  public static void main(String[] args){
    System.err.println("Logs from your program will appear here!");

     String metadataFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
     Path path = Paths.get(metadataFilePath);
     MetadataCache metadataCache = null;
     if(Files.exists(path)) {
         ClusterMetadataProcessor metadataService = new ClusterMetadataProcessor();
         TopicClusterMetadata clusterMetadata = metadataService.loadMetadata(metadataFilePath);
         metadataCache = metadataService.parseMetadata(clusterMetadata);
         System.out.println("Metadata parsed and cached. Ready for requests. \n\n" + metadataCache.toString());
     }
     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     RequestProcessor requestProcessor = new RequestProcessor();
     ResponseProcessor responseProcessor = new ResponseProcessor(supportedApis, metadataCache);
     int port = 9092;
     try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()){
       serverSocket = new ServerSocket(port);
       serverSocket.setReuseAddress(true);
       while (true) {
           clientSocket = serverSocket.accept();
           executor.submit(new ClientHandler(clientSocket, requestProcessor, responseProcessor));
       }
     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     }
  }
}
