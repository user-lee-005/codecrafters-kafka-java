package handlers;

import dto.KafkaRequest;
import processors.RequestProcessor;
import processors.ResponseProcessor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final RequestProcessor requestProcessor;
    private final ResponseProcessor responseProcessor;

    public ClientHandler(Socket clientSocket, RequestProcessor requestProcessor, ResponseProcessor responseProcessor) {
        this.clientSocket = clientSocket;
        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;
    }

    @Override
    public void run() {
        long threadId = Thread.currentThread().threadId();
        try(BufferedInputStream inputStream = new BufferedInputStream(clientSocket.getInputStream())) {
            while(true) {
                DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream));
                KafkaRequest kafkaRequest = requestProcessor.processRequest(dataInputStream);
                System.out.println("[Thread " + threadId + "] Successfully processed request. Corr ID: " + kafkaRequest.getCorrelationId());
                byte[] res = responseProcessor.generateResponse(kafkaRequest);
                responseProcessor.writeToOutputStream(clientSocket, res);
            }
        } catch (EOFException e) {
            System.out.println("Client Disconnected.");
        } catch (IOException e) {
            System.out.println("Client " + clientSocket.getInetAddress() + " disconnected.");
        } catch (Exception e) {
            System.err.println("[Thread " + Thread.currentThread().getId() + "] A critical error occurred, crashing handler!");
            e.printStackTrace(System.err); // This will print the full error stack trace.
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
