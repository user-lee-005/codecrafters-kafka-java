import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import static utils.Constants.messageSize;
import static utils.Constants.unsupportedVersionErrorCode;
import static utils.Utils.getCorrelationId;
import static utils.Utils.getReqApiVersion;

public class Main {
  public static void main(String[] args){
    System.err.println("Logs from your program will appear here!");

     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       serverSocket.setReuseAddress(true);
       clientSocket = serverSocket.accept();
       BufferedInputStream inputStream = new BufferedInputStream(clientSocket.getInputStream());
       int correlationId = getCorrelationId(inputStream);
       short reqApiVersion = getReqApiVersion(inputStream);
       byte[] res = generateResponse(correlationId, reqApiVersion);
       writeToOutputStream(clientSocket, res);
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

    private static byte[] generateResponse(int correlationId, short reqApiVersion) {
        ByteBuffer buf = ByteBuffer.allocate(10);
        writeMessageSize(buf, messageSize);
        writeCorrelationId(buf, correlationId);
        writeReqApiVersion(buf, reqApiVersion);
        return buf.array();
    }

    private static void writeReqApiVersion(ByteBuffer buf, short reqApiVersion) {
        if(reqApiVersion <= 0) {
            buf.putShort(reqApiVersion);
        } else {
            buf.putShort(unsupportedVersionErrorCode);
        }
    }

    private static void writeMessageSize(ByteBuffer buf, int messageSize) {
        buf.putInt(messageSize);
    }

    private static void writeCorrelationId(ByteBuffer buf, int correlationId) {
        buf.putInt(correlationId);
    }

    private static void writeToOutputStream(Socket clientSocket, byte[] res) throws IOException {
        clientSocket.getOutputStream().write(res);
    }
}
