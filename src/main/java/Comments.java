public class Comments {

    /* private static void youMightWanttoScaleUsingBinPackHeterogenous() {
        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        if (size == 0)
            return;
        if (Duration.between(startTime, Instant.now()).toSeconds() <= 140) {

            log.info("Warm up period period has not elapsed yet not taking decisions");
            return;
        }
        if (Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() >= 15) {
            scaleAsPerBinPackHeterogenous(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }*/

   /* public static void scaleAsPerBinPackHeterogenous(int currentsize) {
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScaleHeterogenous();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          *//*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*//*
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
*/
   /* private static int binPackAndScaleHeterogenous() {

        List<Double> capacities = Arrays.asList(100.0, 150.0);
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
                        String.format("%.2f", partition.getArrivalRate()),
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
                if (cons.getRemainingLagCapacity() >= partition.getLag() &&
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
    }
*/


    ////////////////////////////////////////////////////////////////////////

    ////FFD has run on the maximum size/////////


    //System.out.println(bins);
      /*  List<Consumer> newconsumers = new ArrayList<>();
        for (Consumer  c : consumers) {
            for (Double capacity : capacities) {
                if ((c.getArrivalCapacity()- c.getRemainingArrivalCapacity()) <= capacity) {
                    Consumer  newconsumer = new Consumer("id", 0L, capacity);
                    newconsumer.setPartitions(c.getAssignedPartitions());
                    //newconsumer.(b.getCurrentCapacity() - (b.getCapacity() - newbin.getCapacity()));
                    newconsumers.add(newconsumer);
                    break;
                }
            }
        }


        Map<Double, Integer> recommendedConsumerbyCapacity = new HashMap<>();
        Map<Double, Integer> existingConsumerByCapacity = new HashMap<>();


        for(Consumer nc: newconsumers) {
            recommendedConsumerbyCapacity.put(nc.getArrivalCapacity(), recommendedConsumerbyCapacity.getOrDefault(nc, 0) + 1);
        }




        System.out.println(capacities);
        System.out.println(newconsumers);


        ////conusmers to kill//////

        List<Consumer> recommended = newconsumers;
        List<Consumer> existing = assignment;

        for(Double c : capacities) {

            int factor =  - existingConsumerByCapacity.get(c)

        }*/

 /*       recommended.removeAll(existing); // consumer in recommended to be added....

        existing.removeAll(recommended);*/






    //////////////////////////////////////////////////////////////////////


       /* log.info(" The BP scaler recommended {}", consumers.size());
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
        return consumers.size();*/



    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////////////////////////

 /*   private static void youMightWanttoScaleUsingBinPack() {
        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        if (size == 0)
            return;
        if (Duration.between(startTime, Instant.now()).toSeconds() <= 140) {

            log.info("Warm up period period has not elapsed yet not taking decisions");
            return;
        }
        if (Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() >= 15) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }*/

   /* public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          *//*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*//*
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
                        String.format("%.2f", partition.getArrivalRate()),
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
                if (cons.getRemainingLagCapacity() >= partition.getLag() &&
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
        List<Partition> fairpartitions = new ArrayList<>();

        for (Consumer cons : consumers) {
            fairconsumers.add(new Consumer(cons.getId(), maxLagCapacity, dynamicAverageMaxConsumptionRate));
            for (Partition p : cons.getAssignedPartitions()) {
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
        assignPartitionsFairly(fairconsumers, consumers, fairpartitions);
        for (Consumer cons : fairconsumers) {
            log.info("fair consumer {} is assigned the following partitions", cons.getId());
            for (Partition p : cons.getAssignedPartitions()) {
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
                            Double.compare(c1.getValue(), c2.getValue()) != 0 ?
                                    Double.compare(c1.getValue(), c2.getValue()) : c1.getKey().compareTo(c2.getKey())).getKey();

            int memberIndex;
            for (memberIndex = 0; memberIndex < consumers.size(); memberIndex++) {
                if (assignment.get(memberIndex).getId().equals(memberId)) {
                    break;
                }
            }

            assignment.get(memberIndex).assignPartition(partition);
            consumerTotalArrivalRate.put(memberId, consumerTotalArrivalRate.getOrDefault(memberId, 0.0) + partition.getArrivalRate());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);
            log.info(
                    "Assigned partition {} to consumer {}.  partition_arrival_rate={}, consumer_current_total_arrival_rate{} ",
                    partition.getId(),
                    memberId,
                    String.format("%.2f", partition.getArrivalRate()),
                    consumerTotalArrivalRate.get(memberId));
        }
    }*/


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
