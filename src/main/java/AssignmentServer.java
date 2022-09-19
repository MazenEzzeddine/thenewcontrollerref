import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AssignmentServer implements Runnable {

    private final int port;
    private final Server server;
    private static final Logger log = LogManager.getLogger(AssignmentServer.class);

    public AssignmentServer(int port) throws IOException {
        this(ServerBuilder.forPort(port), port);
    }

    public AssignmentServer(ServerBuilder<?> serverBuilder, int port) {
        this.port = port;
        this.server = serverBuilder.addService(new AssignmentService()).build();
    }

    public void start() throws IOException {
        log.info("Server Started");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                log.info("*** shutting down gRPC server since JVM is shutting down");
                AssignmentServer.this.stop();
                log.info("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void run() {
        try {
            start();
            blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static boolean firsttime = true;

    public static class AssignmentService extends AssignmentServiceGrpc.AssignmentServiceImplBase {
        @Override
        public void getAssignment(AssignmentRequest request, StreamObserver<AssignmentResponse> responseObserver) {
            log.info(request.getRequest());
            //TODO Synchronize access to assignment
            List<Consumer> assignment = new ArrayList<>();//PrometheusHttpClient.assignment;
            //log.info("The assignment is {}", assignment);


            //if(assignment.size() == 0) {

            if (firsttime) {


               Consumer c0 = new Consumer(100.0);
               c0.setId("cons100-0");
                //Consumer c1 = new Consumer("cons200-0",500L,100);

                c0.assign(new Partition(0, 0L,0.0d));
                c0.assign(new Partition(1, 0L,0.0d));
                c0.assign(new Partition(2, 0L,0.0d));
                c0.assign(new Partition(3, 0L,0.0d));
                c0.assign(new Partition(4, 0L,0.0d));
                assignment.add(c0);
                //assignment.add(c1);
                firsttime = false;

             /*   Consumer c0 = new Consumer("0",500L,100);
                Consumer c1 = new Consumer("1",500L,100);
                Consumer c2 = new Consumer("2",500L,100);
                Consumer c3 = new Consumer("3",500L,100);
                Consumer c4 = new Consumer("4",500L,100);
                c0.assignPartition(new Partition(0, 0L,0.0d));
                c1.assignPartition(new Partition(1, 0L,0.0d));
                c2.assignPartition(new Partition(2, 0L,0.0d));
                c3.assignPartition(new Partition(3, 0L,0.0d));
                c4.assignPartition(new Partition(4, 0L,0.0d));


                assignment.add(c0);
                assignment.add(c1);
                assignment.add(c2);
                assignment.add(c3);
                assignment.add(c4);
*/


            }

            // else {
            //assignment.clear();


            //Consumer c0 = new Consumer("cons100-0",500L,100);
               /* Consumer c1 = new Consumer("cons200-0",500L,100);

                c1.assignPartition(new Partition(0, 0L,0.0d));
                c1.assignPartition(new Partition(1, 0L,0.0d));
                c1.assignPartition(new Partition(2, 0L,0.0d));
                c1.assignPartition(new Partition(3, 0L,0.0d));
                c1.assignPartition(new Partition(4, 0L,0.0d));
                //assignment.add(c0);
                assignment.add(c1);

            }*/
            List<ConsumerGrpc> assignmentReply = new ArrayList<>(assignment.size());

            for (Consumer cons : assignment) {
                List<PartitionGrpc> pgrpclist = new ArrayList<>();
                for (Partition p : cons.getAssignedPartitions()) {
                    log.info("partition {} is assigned to consumer {}", p.getId(), cons.getId());
                   PartitionGrpc pgrpc =  PartitionGrpc.newBuilder().setId(p.getId()).build();
                   pgrpclist.add(pgrpc);
                }
                ConsumerGrpc consg  =  ConsumerGrpc.newBuilder().setId(cons.getId()).addAllAssignedPartitions(pgrpclist).build();
                assignmentReply.add(consg);
            }

            responseObserver.onNext(AssignmentResponse.newBuilder().addAllConsumers(assignmentReply).build());
            responseObserver.onCompleted();
            log.info("Sent Assignment to client");
            //PrometheusHttpClient.joiningTime = Duration.between(PrometheusHttpClient.lastScaleTime, Instant.now()).getSeconds();
            //log.info("joiningTime {}", PrometheusHttpClient.joiningTime);*/
            // }
        }

    }
}