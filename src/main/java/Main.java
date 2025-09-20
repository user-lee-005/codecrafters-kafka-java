import dto.KafkaRequest;
import dto.KafkaResponse;
import processors.RequestProcessor;
import processors.ResponseProcessor;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Main {
  static final List<KafkaResponse.ApiVersionDTO> supportedApis = List.of(
          new KafkaResponse.ApiVersionDTO((short)18, (short)0, (short)4)    // ApiVersions
  );

  public static void main(String[] args){
    System.err.println("Logs from your program will appear here!");

     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     RequestProcessor requestProcessor = new RequestProcessor();
     ResponseProcessor responseProcessor = new ResponseProcessor(supportedApis);
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       serverSocket.setReuseAddress(true);
       clientSocket = serverSocket.accept();
       BufferedInputStream inputStream = new BufferedInputStream(clientSocket.getInputStream());
       while(true)
       {
           KafkaRequest kafkaRequest = requestProcessor.processRequest(inputStream);
           byte[] res = responseProcessor.generateResponse(kafkaRequest);
           responseProcessor.writeToOutputStream(clientSocket, res);
       }
     } catch (EOFException e) {
       System.out.println("Client Disconnected.");
     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }
}
