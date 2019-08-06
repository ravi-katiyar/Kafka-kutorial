# Kafka-kutorial
Kafka - All concepts and functionality explained through sample Java Codes

We can find sample Java code here for 

1. Demo Kafka Producer --> ProducerDemo.java
2. Kafka Producer with CallBacks --> ProducerDemoWithCallback.java
3. Kafka Producer with Keys --> ProducerDemoKeys.java

# IMPORTANT POINTS 
1. At any time only 1 broker can be a leader for a partition and only that leader can receive and send the data for the partition.
2. The Other broker will synchronize the data.
3. Therefore each partition has 1 leader and multiple ISR(in-sync-replica)
