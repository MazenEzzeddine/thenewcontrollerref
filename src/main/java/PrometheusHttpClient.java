import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class PrometheusHttpClient  implements Runnable{

    private static final Logger log = LogManager.getLogger(PrometheusHttpClient.class);

    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static Long sleep;
    static String topic;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    static int size;
    static ArrayList<Partition> topicpartitions = new ArrayList<>();
    static double dynamicAverageMaxConsumptionRate = 0.0;

    static double wsla = 5.0;
    static List<Consumer> assignment = new ArrayList<>();
    static Instant lastScaleUpDecision;
    static Instant lastScaleDownDecision;
    static Instant lastCGQuery;
    static Instant startTime;
    static Integer cooldown;

    private static  void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(PrometheusHttpClient.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
         size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("number of consumers {}", size );
    }

    private static void readEnvAndCrateAdminClient() {
        log.info("inside read env");
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////

    private static void youMightWanttoScaleUsingBinPack() {
        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        if(size==0)
            return;
        if(Duration.between(startTime, Instant.now()).toSeconds() <= 140 ) {

            log.info("Warm up period period has not elapsed yet not taking decisions");
            return;
        }
        if(Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() >= 15 ) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }

    public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          /*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Upscaled you should have {}", neededsize);
            }
            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
        } else {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Downscaled you should have {}", neededsize);
                lastScaleUpDecision = Instant.now();
                lastScaleDownDecision = Instant.now();
                lastCGQuery = Instant.now();
            }
        }
    }


    private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(topicpartitions);
        dynamicAverageMaxConsumptionRate = 95.0;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);
        consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity, false);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate, false);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {
                //TODO externalize these choices on the inout to the FFD bin pack
                // TODO  hey stupid use instatenous lag instead of average lag.
                // TODO average lag is a decision on past values especially for long DI.
                if (cons.getRemainingLagCapacity() >=  partition.getLag()  &&
                        cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer((String.valueOf(consumerCount)), (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }
        log.info(" The BP scaler recommended {}", consumers.size());
        // copy consumers and partitions for fair assignment
        List<Consumer> fairconsumers = new ArrayList<>(consumers.size());
        List<Partition> fairpartitions= new ArrayList<>();

        for (Consumer cons : consumers) {
            fairconsumers.add(new Consumer(cons.getId(), maxLagCapacity, dynamicAverageMaxConsumptionRate));
            for(Partition p : cons.getAssignedPartitions()){
                fairpartitions.add(p);
            }
        }

        //sort partitions in descending order for debugging purposes
        fairpartitions.sort(new Comparator<>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                return Double.compare(o2.getArrivalRate(), o1.getArrivalRate());
            }
        });

        //1. list of consumers that will contain the fair assignment
        //2. list of consumers out of the bin pack.
        //3. the partition sorted in their decreasing arrival rate.
        assignPartitionsFairly(fairconsumers,consumers,fairpartitions);
        for (Consumer cons : fairconsumers) {
            log.info("fair consumer {} is assigned the following partitions", cons.getId() );
            for(Partition p : cons.getAssignedPartitions()) {
                log.info("fair Partition {}", p.getId());
            }
        }
        assignment = fairconsumers;
        return consumers.size();
    }










    /////////////////////////////////////////////////////////////////////////////////////////////////////////


    public static void assignPartitionsFairly(
            final List<Consumer> assignment,
            final List<Consumer> consumers,
            final List<Partition> partitionsArrivalRate) {
        if (consumers.isEmpty()) {
            return;
        }// Track total lag assigned to each consumer (for the current topic)
        final Map<String, Double> consumerTotalArrivalRate = new HashMap<>(consumers.size());
        final Map<String, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        final Map<String, Double> consumerAllowableArrivalRate = new HashMap<>(consumers.size());
        for (Consumer cons : consumers) {
            consumerTotalArrivalRate.put(cons.getId(), 0.0);
            consumerAllowableArrivalRate.put(cons.getId(), 95.0);
            consumerTotalPartitions.put(cons.getId(), 0);

        }

        // might want to remove, the partitions are sorted anyway.
        //First fit decreasing
        partitionsArrivalRate.sort((p1, p2) -> {
            // If lag is equal, lowest partition id first
            if (p1.getArrivalRate() == p2.getArrivalRate()) {
                return Integer.compare(p1.getId(), p2.getId());
            }
            // Highest arrival rate first
            return Double.compare(p2.getArrivalRate(), p1.getArrivalRate());
        });
        for (Partition partition : partitionsArrivalRate) {
            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id arrival rate
            // returns the consumer with lowest assigned partitions, if all assigned partitions equal returns the min total arrival rate
            final String memberId = Collections
                    .min(consumerTotalArrivalRate.entrySet(), (c1, c2) ->
                            Double.compare(c1.getValue(), c2.getValue())!=0?
                            Double.compare(c1.getValue(), c2.getValue()): c1.getKey().compareTo(c2.getKey())).getKey();

            int memberIndex;
            for( memberIndex = 0; memberIndex<consumers.size(); memberIndex++) {
                if(assignment.get(memberIndex).getId().equals(memberId)){
                    break;
                }
            }

            assignment.get(memberIndex).assignPartition(partition);
            consumerTotalArrivalRate.put(memberId, consumerTotalArrivalRate.getOrDefault(memberId, 0.0) + partition.getArrivalRate());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);
            log.info(
                    "Assigned partition {} to consumer {}.  partition_arrival_rate={}, consumer_current_total_arrival_rate{} ",
                    partition.getId(),
                    memberId ,
                    String.format("%.2f", partition.getArrivalRate()) ,
                    consumerTotalArrivalRate.get(memberId));
        }
    }







    private static Double parseJsonArrivalRate(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        ///String partition = jobjpartition.getString("partition");
        /*log.info("the partition is {}", p);
        log.info("partition arrival rate: {}", Double.parseDouble( jreq.getString(1)));*/
        return Double.parseDouble( jreq.getString(1));
    }


    private static Double parseJsonArrivalLag(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
       /* log.info("the partition is {}", p);
        log.info("partition lag  {}",  Double.parseDouble( jreq.getString(1)));*/
        return Double.parseDouble( jreq.getString(1));
    }








    @Override
    public void run() {
        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();
        lastScaleUpDecision=  Instant.now();
        lastScaleDownDecision = Instant.now();
        startTime = Instant.now();
        log.info("Sleeping for 1.5 minutes to warmup");
        HttpClient client = HttpClient.newHttpClient();
        String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
        String p0 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B1m%5D))";
        String p1 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B1m%5D))";
        String p2 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B1m%5D))";
        String p3 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B1m%5D))";
        String p4 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B1m%5D))";
       /* String p5 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%225%22,namespace=%22default%22%7D%5B1m%5D))";
        String p6 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%226%22,namespace=%22default%22%7D%5B1m%5D))";
        String p7 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%227%22,namespace=%22default%22%7D%5B1m%5D))";
        String p8 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%228%22,namespace=%22default%22%7D%5B1m%5D))";
        String p9 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%229%22,namespace=%22default%22%7D%5B1m%5D))";
        String p10 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%2210%22,namespace=%22default%22%7D%5B1m%5D))";
        String p11 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%2211%22,namespace=%22default%22%7D%5B1m%5D))";*/


        //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
        //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

        String all4 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
        String p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D";
        String p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D";
        String p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D";
        String p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D";
        String p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D";
       /* String p5lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%225%22,namespace=%22default%22%7D";
        String p6lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%226%22,namespace=%22default%22%7D";
        String p7lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%227%22,namespace=%22default%22%7D";
        String p8lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%228%22,namespace=%22default%22%7D";
        String p9lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%229%22,namespace=%22default%22%7D";
        String p10lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%2210%22,namespace=%22default%22%7D";
        String p11lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%2211%22,namespace=%22default%22%7D";*/


        List<URI> partitions= new ArrayList<>();
        try {
            partitions = Arrays.asList(
                    new URI(p0),
                    new URI(p1),
                    new URI(p2),
                    new URI(p3),
                    new URI(p4)
                   /* new URI(p5),
                    new URI(p6),
                    new URI(p7),
                    new URI(p8),
                    new URI(p9),
                    new URI(p10),
                    new URI(p11)*/
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag= new ArrayList<>();
        try {
            partitionslag = Arrays.asList(
                    new URI(p0lag),
                    new URI(p1lag),
                    new URI(p2lag),
                    new URI(p3lag),
                    new URI(p4lag)
                   /* new URI(p5lag),
                    new URI(p6lag),
                    new URI(p7lag),
                    new URI(p8lag),
                    new URI(p9lag),
                    new URI(p10lag),
                    new URI(p11lag)*/
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        for (int i = 0; i<=4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }
       // log.info("created the 5 partitions");

        while(true) {
            Instant start = Instant.now();

            List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                    .map(target -> client
                            .sendAsync(
                                    HttpRequest.newBuilder(target).GET().build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body))
                    .collect(Collectors.toList());


            List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                    .map(target -> client
                            .sendAsync(
                                    HttpRequest.newBuilder(target).GET().build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body))
                    .collect(Collectors.toList());



            int partitionn = 0;
            double totalarrivals=0.0;
            for (CompletableFuture cf : partitionsfutures) {
                try {
                    topicpartitions.get(partitionn).setArrivalRate(parseJsonArrivalRate((String) cf.get(), partitionn), false);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                try {
                    totalarrivals += parseJsonArrivalRate((String) cf.get(), partitionn);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                partitionn++;
            }
            log.info("totalArrivalRate {}", totalarrivals);



            partitionn = 0;
            double totallag=0.0;
            for (CompletableFuture cf : partitionslagfuture) {
                try {
                    topicpartitions.get(partitionn).setLag(parseJsonArrivalLag((String) cf.get(), partitionn).longValue(), false);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                try {
                    totallag += parseJsonArrivalLag((String) cf.get(), partitionn);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                partitionn++;
            }


            log.info("totalLag {}", totallag);
            Instant end = Instant.now();
            log.info("Duration in seconds to query prometheus for " +
                            "arrival rate and lag and parse result {}",
                    Duration.between(start,end).toMillis());


            for (int i = 0; i<=4; i++) {
                log.info("partition {} has the following arrival rate {} and lag {}",  i, topicpartitions.get(i).getArrivalRate(),
                        topicpartitions.get(i).getLag()) ;
            }


            log.info("calling the scaler");


            try {
                queryConsumerGroup();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }

            youMightWanttoScaleUsingBinPack();
           /* log.info("calling youmightwanttoscaler (linear), arrivals {}", totalarrivals);
            try {
                youMightWanttoScale(totalarrivals);
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }*/
            log.info("sleeping for 30 s");
            log.info("==================================================");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static void youMightWanttoScale(double totalArrivalRate) throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);

        if(size==0)
            return;
        if(Duration.between(startTime, Instant.now()).toSeconds() <= 140 ) {

            log.info("Warm up period period has not elapsed yet not taking decisions");
            return;
        }


        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 30) {
            log.info("Upscale logic, Up scale cool down has ended");

            scale(totalArrivalRate, size);
        }
    }


    private static void scale (double totalArrivalRate,int size) {

        int reco = (int) Math.ceil(totalArrivalRate / poll);
        log.info("recommended number of replicas {}", reco);

        if (reco != size) {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(reco);

            }

        }

        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();


        log.info("S(int) Math.ceil(totalArrivalRate / poll) {}  ", reco );
    }
}
