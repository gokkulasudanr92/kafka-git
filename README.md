# kafka-git
Learning Kafka

##Beginnig
java -Dfile.encoding=UTF-8 -classpath /home/gsudan/eclipse-workspace/beginner/target/classes:/home/gsudan/.m2/repository/org/apache/kafka/kafka-clients/2.2.1/kafka-clients-2.2.1.jar:/home/gsudan/.m2/repository/com/github/luben/zstd-jni/1.3.8-1/zstd-jni-1.3.8-1.jar:/home/gsudan/.m2/repository/org/lz4/lz4-java/1.5.0/lz4-java-1.5.0.jar:/home/gsudan/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.2/snappy-java-1.1.7.2.jar:/home/gsudan/.m2/repository/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:/home/gsudan/.m2/repository/org/slf4j/slf4j-simple/1.7.26/slf4j-simple-1.7.26.jar com.gsudan.kafka.consumer.ConsumerDemoWithThreads

## Twitter Data Consumer
Real-World Exercise:

Before jumping to the next section for the solution, here are some pointers for some exercises:

Twitter Producer

The Twitter Producer gets data from Twitter based on some keywords and put them in a Kafka topic of your choice

    Twitter Java Client: https://github.com/twitter/hbc

    Twitter API Credentials: https://developer.twitter.com/

ElasticSearch Consumer

The ElasticSearch Consumer gets data from your twitter topic and inserts it into ElasticSearch

    ElasticSearch Java Client: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.4/java-rest-high.html

    ElasticSearch setup:

        https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html

        OR https://bonsai.io/

