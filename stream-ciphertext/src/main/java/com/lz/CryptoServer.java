package com.lz;

import com.lz.rpc.CryptoServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/5 10:50
 * @version: 1.8
 */
public class CryptoServer {
    private static final int PORT = 9090;
    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new CryptoServiceImpl())
                .build()
                .start();
        System.out.println("Server started, listening on " + PORT);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down grpc server");
            if (server != null){
                server.shutdownNow();
            }
        }));

        server.awaitTermination();
    }
}
