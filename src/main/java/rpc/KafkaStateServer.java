package rpc;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import prices.KakfaStateService;

public class KafkaStateServer {

    public final static int APPLICATION_SERVER_PORT = 9090;

    public static KafkaStateHandler handler;

    public static KakfaStateService.Processor processor;

    public static void run(KafkaStreams stream) {
        try {
            handler = new KafkaStateHandler(stream);
            processor = new KakfaStateService.Processor(handler);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };

            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(KakfaStateService.Processor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(APPLICATION_SERVER_PORT);
            TServer.Args args = new TServer.Args(serverTransport)
                    .processor(processor)
                    .inputProtocolFactory(new TJSONProtocol.Factory())
                    .outputProtocolFactory(new TJSONProtocol.Factory());

            TServer server = new TSimpleServer(args);

            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
