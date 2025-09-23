import dto.KafkaResponse;
import handlers.ClientHandler;
import processors.RequestProcessor;
import processors.ResponseProcessor;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  static final List<KafkaResponse.ApiVersionDTO> supportedApis = List.of(
          new KafkaResponse.ApiVersionDTO((short)18, (short)0, (short)4),    // ApiVersions
//          new KafkaResponse.ApiVersionDTO((short)1, (short)0, (short)4),    // Fetch
          new KafkaResponse.ApiVersionDTO((short)75, (short)0, (short)0)    // DescribeTopicPartitions
  );

  public static void main(String[] args){
    System.err.println("Logs from your program will appear here!");

     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     RequestProcessor requestProcessor = new RequestProcessor();
     ResponseProcessor responseProcessor = new ResponseProcessor(supportedApis);
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
