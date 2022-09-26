import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
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


/////////////////////////////////////////////////////


/////////////////////////////////////////////////////


public class PrometheusHttpClient  implements Runnable {

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
    static Map<Double, Integer> previousConsumers = new HashMap<>();

    static Map<Double, Integer> currentConsumers =  new HashMap<>();

    final static      List<Double> capacities = Arrays.asList(95.0, 240.0);
    public static List<Consumer> newassignment = new ArrayList<>();


    public static  Instant warmup = Instant.now();


    static Instant lastScaletime;



    ////////////////////////////////////////////////////////////////////////
    static TopicDescription td;
    static DescribeTopicsResult tdr;
    static ArrayList<Partition> partitions = new ArrayList<>();


    ////////////////////////////////////////////////////////////////////////


    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(PrometheusHttpClient.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
        size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("number of consumers {}", size);
    }

    private static void readEnvAndCrateAdminClient() {
        log.info("inside read env");

        for (double c : capacities) {
            currentConsumers.put(c, 0);
            previousConsumers.put(c,0);
        }
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
       previousConsumers.put(100.0, 1);
    }




    private static Double parseJsonArrivalRate(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        ///String partition = jobjpartition.getString("partition");
        /*log.info("the partition is {}", p);
        log.info("partition arrival rate: {}", Double.parseDouble( jreq.getString(1)));*/
        return Double.parseDouble(jreq.getString(1));
    }


    private static Double parseJsonArrivalLag(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
       /* log.info("the partition is {}", p);
        log.info("partition lag  {}",  Double.parseDouble( jreq.getString(1)));*/
        return Double.parseDouble(jreq.getString(1));
    }


    @Override
    public void run() {
        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();
        lastScaleUpDecision = Instant.now();
        lastScaleDownDecision = Instant.now();
        startTime = Instant.now();
        log.info("Sleeping for 1.5 minutes to warmup");
        HttpClient client = HttpClient.newHttpClient();



        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
        String p0 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B1m%5D))";
        String p1 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B1m%5D))";
        String p2 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B1m%5D))";
        String p3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B1m%5D))";
        String p4 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B1m%5D))";


        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

       /* String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B5s%5D))%20by%20(topic)";
        String p0 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B5s%5D))";
        String p1 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B5s%5D))";
        String p2 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B5s%5D))";
        String p3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B5s%5D))";
        String p4 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B5s%5D))";*/










        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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


        List<URI> partitions = new ArrayList<>();
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
        List<URI> partitionslag = new ArrayList<>();
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


        for (int i = 0; i <= 4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }
        // log.info("created the 5 partitions");



        log.info("Thread.sleep(140*1000)");

        try {
            //Initial delay so that the producer has started.
            lastScaletime = Instant.now();
            Thread.sleep(140*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }




        while (true) {
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
            double totalarrivals = 0.0;
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
            double totallag = 0.0;
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
                    Duration.between(start, end).toMillis());


            for (int i = 0; i <= 4; i++) {
                log.info("partition {} has the following arrival rate {} and lag {}", i, topicpartitions.get(i).getArrivalRate(),
                        topicpartitions.get(i).getLag());
            }


            log.info("calling the scaler");


           /* try {
                queryConsumerGroup();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }*/
         /*   try {
                youMightWanttoScaleTrial2();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }*/

            //youMightWanttoScaleUsingBinPack();
            //youMightWanttoScaleUsingBinPackHeterogenous();
           /* log.info("calling youmightwanttoscaler (linear), arrivals {}", totalarrivals);
            try {
                youMightWanttoScale(totalarrivals);
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }*/



            if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 30)
                youMightWanttoScaleTrial2();


            log.info("sleeping for 5 s");
            log.info("==================================================");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }









    public static void  youMightWanttoScaleTrial2(){

        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(topicpartitions);
        Map<Double, List<Consumer>> currentConsumersByName = new HashMap<>();
    /*   LeastLoadedFFD llffd = new LeastLoadedFFD(parts, 95.0);
        List<Consumer> cons = llffd.LeastLoadFFDHeterogenous();*/

        FirstFitDecHetero hetero = new FirstFitDecHetero(parts, capacities);
        List<Consumer> cons = hetero.fftFFDHetero();


        for(double c : capacities) {
            currentConsumersByName.putIfAbsent(c, new ArrayList<>());

        }


        log.info("we currently need this consumer");
        log.info(cons);
        newassignment.clear();

        for (Consumer co: cons) {
            log.info(co.getCapacity());
            currentConsumers.put(co.getCapacity(), currentConsumers.get(co.getCapacity()) +1);
            currentConsumersByName.get(co.getCapacity()).add(co);
            // currentConsumersByName.put(co.getCapacity(), co);
        }

        for (double d : currentConsumers.keySet()) {
            log.info("current consumer capacity {}, {}", d, currentConsumers.get(d));
        }

        Map<Double, Integer> scaleByCapacity = new HashMap<>();
        Map<Double, Integer>  diffByCapacity = new HashMap<>();

        for (double d : currentConsumers.keySet()) {
            if (currentConsumers.get(d).equals(previousConsumers.get(d))) {
                log.info("No need to scale consumer of capacity {}", d);
            }


            int index=0;
            for (Consumer c:  currentConsumersByName.get(d)) {
                c.setId("cons"+(int)d+ "-" + index);
                index++;
                log.info(c.getId());
            }



            int factor = currentConsumers.get(d); /*- previousConsumers.get(d);*/
            int  diff = currentConsumers.get(d) - previousConsumers.get(d);
            log.info("diff {} for capacity {}", diff, d);
            diffByCapacity.put(d, diff);

            scaleByCapacity.put(d, factor);
            log.info(" the consumer of capacity {} shall be scaled to {}", d, factor);
        }

        newassignment.addAll(cons);







       /* for (double d : capacities) {
            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d)!=0) {
                log.info("The statefulset {} shall be  scaled to {}", "cons"+(int)d, scaleByCapacity.get(d) );
                if(Duration.between(warmup, Instant.now()).toSeconds() > 30 ) {
                    log.info("cons"+(int)d);

                    new Thread(()-> { try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons"+(int)d).scale(scaleByCapacity.get(d));
                    }}).start();
                    lastScaletime = Instant.now();
                }
            }
        }
*/

        for (double d : capacities) {
            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d) > 0) {
                log.info("The statefulset {} shall be  scaled to {}", "cons"+(int)d, scaleByCapacity.get(d) );
                if(Duration.between(warmup, Instant.now()).toSeconds() > 30 ) {
                    log.info("cons"+(int)d);

                    /* new Thread(()-> {*/ try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons"+(int)d).scale(scaleByCapacity.get(d));
                    }}/*).start();*/
                lastScaletime = Instant.now();
            }
        }



        for (double d : capacities) {
            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d) < 0) {
                log.info("The statefulset {} shall be  scaled to {}", "cons"+(int)d, scaleByCapacity.get(d) );
                if(Duration.between(warmup, Instant.now()).toSeconds() > 30 ) {
                    log.info("cons"+(int)d);

                    /* new Thread(()-> {*/ try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons"+(int)d).scale(scaleByCapacity.get(d));
                    }}/*).start();*/
                lastScaletime = Instant.now();
            }
        }


        for (double d : capacities) {

            previousConsumers.put(d, currentConsumers.get(d));
            currentConsumers.put(d, 0);
        }

    }















    static      boolean onetime = false;
    private static void youMightWanttoScaleTrial() throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);


        if (size == 0)
            return;
        if (Duration.between(startTime, Instant.now()).toSeconds() <= 60) {

            log.info("Warm up period period has not elapsed yet not taking decisions");
            return;

        } else {
            if(!onetime) {

                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    //k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(reco);
                    k8s.apps().statefulSets().inNamespace("default").withName("cons100").scale(0);
                }
                onetime = true;
            }


       /* if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 30) {
            log.info("Upscale logic, Up scale cool down has ended");

            scale(totalArrivalRate, size);
        }*/
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
