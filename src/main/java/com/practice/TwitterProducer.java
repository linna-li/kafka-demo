package com.practice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.component.ComponentsBuilderFactory;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For Twitter!
 */
public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) throws Exception {
        new TwitterProducer().runByCamel();
    }

    public void runByRawKafka() {
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        // create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                // twitter_tweets是topic的名字
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("something bad happen", e);
                        }
                    }
                });
            }
        }
        logger.info("END");
    }

    public void runByCamel() throws Exception {
        logger.info("Setup");
        useCammelProduceMessage("mockMessage");
    }

    // Need to add key and secret if want to use twitter
    String consumerKey = "consumerKey";
    String consumerSecret = "consumerSecret";
    String token = "token";
    String secret = "secret";

    // Client need to know which message queue it should put message in.
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        // 连接kafka，把推特传到kafka里面去
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);

    }

    // camel 让你能使用相同的api和处理流程，处理不同协议和数据类型的系统。
    // camel实现了客户端与服务端的解耦， 生产者和消费者的解耦。
    public void useCammelProduceMessage(String message) throws Exception {
        try (CamelContext camelContext = new DefaultCamelContext()) {
            camelContext.addRoutes(new RouteBuilder() {
                public void configure() {
                    // camelContext.getPropertiesComponent().setLocation("classpath:application.properties");
                    ComponentsBuilderFactory.kafka().brokers("127.0.0.1:9092").register(camelContext, "kafka");
                    from("direct:kafkaStart").routeId("DirectToKafka").to("kafka:twitter_tweets").log("${headers}");
                }
            });

            try (ProducerTemplate producerTemplate = camelContext.createProducerTemplate()) {
                camelContext.start();
                Map<String, Object> headers = new HashMap<>();
                headers.put(KafkaConstants.PARTITION_KEY, 0);
                headers.put(KafkaConstants.KEY, "1");
                producerTemplate.sendBodyAndHeaders("direct:kafkaStart", message, headers);
            }

            logger.info("Successfully published event to Kafka.");
            camelContext.stop();
        }

    }
}
