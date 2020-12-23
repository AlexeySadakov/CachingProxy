package org.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class CachingProxyApplication {
    private static final Logger logger = Logger.getLogger("CachingProxy");
    private static final Queue<String> queue = new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean serviceAvailable = new AtomicBoolean(false);


    public static void main(String[] args) throws IOException, URISyntaxException {
        try (InputStream is = CachingProxyApplication.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is == null) {
                System.out.println("Unable to find application.properties");
                return;
            }
            Properties properties = new Properties();
            properties.load(is);

            int serverPort = Integer.parseInt(properties.getProperty("serverPort"));
            int intervalCheckHealth = Integer.parseInt(properties.getProperty("intervalCheckHealth"));
            String contextSubmitData = properties.getProperty("contextSubmitData");
            String requestRealServiceCheckHealthyURI = properties.getProperty("requestRealServiceCheckHealthyURI");
            String requestRealServiceSubmitDataURI = properties.getProperty("requestRealServiceSubmitDataURI");

            HttpServer server = HttpServer.create(new InetSocketAddress(serverPort), 0);
            server.createContext(contextSubmitData, new SubmitDataHandler(logger));

            ExecutorService cachingProxyServerExecutor = Executors.newCachedThreadPool();
            server.setExecutor(cachingProxyServerExecutor);
            server.start();

            Runnable realServiceDataSender = getRealServiceDataSender(requestRealServiceSubmitDataURI);
            ExecutorService realServiceExecutor = Executors.newFixedThreadPool(1);
            realServiceExecutor.submit(realServiceDataSender);

            Runnable realServiceHealthChecker = getRealServiceHealthChecker(requestRealServiceCheckHealthyURI, intervalCheckHealth);
            ExecutorService realServiceHealthCheckingExecutor = Executors.newFixedThreadPool(1);
            realServiceHealthCheckingExecutor.submit(realServiceHealthChecker);


        }

    }

    private static Runnable getRealServiceHealthChecker(String requestCheckHealthyURI, int intervalCheckHealth) throws URISyntaxException {
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest requestCheckHealthy = HttpRequest.newBuilder()
                .uri(new URI(requestCheckHealthyURI))
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();

        return () -> {
            try {
                while (true) {
                    client.sendAsync(requestCheckHealthy,
                            HttpResponse.BodyHandlers.ofString())
                            .whenComplete((s, t) -> {
                                if (t == null && s.statusCode() == 200) {
                                        System.out.println("Service available: " + s);
                                        serviceAvailable.set(true);
                                        synchronized (queue) {
                                            queue.notify();
                                        }
                                } else {
                                    System.err.println("Service unavailable: " + t);
                                    serviceAvailable.set(false);
                                }
                            });

                    Thread.sleep(intervalCheckHealth);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }


    private static Runnable getRealServiceDataSender(String requestRealServiceSubmitDataURI) {
        HttpClient client = HttpClient.newBuilder().build();

        return () -> {
            try {
                while (true) {
                    if (serviceAvailable.get()) {
                        if (queue.isEmpty())
                            logger.info("Queue is empty. Nothing to send.");
                        else {
                            while (!queue.isEmpty()) {
                                String data = queue.poll();
                                logger.info("Trying to send: " + data);

                                HttpRequest requestRealServiceSubmitData = HttpRequest.newBuilder()
                                        .uri(new URI(requestRealServiceSubmitDataURI + "?" + data))
                                        .GET()
                                        .timeout(Duration.ofSeconds(10))
                                        .build();

                                client.sendAsync(requestRealServiceSubmitData,
                                        HttpResponse.BodyHandlers.ofString())
                                        .whenComplete((s, t) -> {
                                            if (t != null || s.statusCode() != 200) {
                                                logger.info("Error while sending data: " + data);
                                                queue.offer(data);
                                            } else {
                                                logger.info("Successfully sent data: " + data);
                                            }
                                        });
                            }
                        }
                    } else {
                        logger.warning("Service is unavailable");
                    }
                    synchronized (queue) {
                        queue.wait();
                    }
                }
            } catch (InterruptedException | URISyntaxException e) {
                e.printStackTrace();
            }
        };
    }

    private static void prepareExchangeResponse(HttpExchange exchange, String text) throws IOException {
        exchange.sendResponseHeaders(200, text.getBytes().length);
        OutputStream output = exchange.getResponseBody();
        output.write(text.getBytes());
        output.flush();
    }

static class SubmitDataHandler implements HttpHandler {
    private final Logger logger;

    public SubmitDataHandler(Logger logger) {
        this.logger = logger;
    }

    public void handle(HttpExchange exchange) throws IOException {
        String threadName = " "
                + Thread.currentThread().getName() + " / "
                + Thread.currentThread().getId();
        logger.info("Serving the request from Thread" + threadName);
        String rawQuery = exchange.getRequestURI().getRawQuery();
        queue.offer(rawQuery);
        logger.info(String.join(",", queue));
        prepareExchangeResponse(exchange, threadName);
        exchange.close();
        logger.info("Continue request in Thread " + threadName);
        synchronized (queue) {
            queue.notify();
        }
    }

}

}
