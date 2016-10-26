import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.topic.TopicOverloadPolicy;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by aspear on 10/26/16.
 */
public class HazelcastTopicDemo implements MessageListener<String> {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static HazelcastInstance hazelcastInstance;

    public static void main( String[] args ) {
        HazelcastTopicDemo sample = new HazelcastTopicDemo();

        hazelcastInstance = Hazelcast.newHazelcastInstance();
        RingbufferConfig ringbufferConfig = hazelcastInstance.getConfig().getRingbufferConfig("default");
        ringbufferConfig.setCapacity(100000);
        ringbufferConfig.setTimeToLiveSeconds(0);

        ReliableTopicConfig reliableTopicConfig =hazelcastInstance.getConfig().getReliableTopicConfig("default");
        reliableTopicConfig.setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);


        ITopic topic = hazelcastInstance.getReliableTopic( "default" );
        topic.addMessageListener( sample );
        sample.scheduler.scheduleAtFixedRate(
          () -> topic.publish( new Date().toString() + " from " +hazelcastInstance.getCluster().getLocalMember().getSocketAddress().getPort()
          ), 1, 10, TimeUnit.SECONDS);

    }

    public void onMessage( Message<String> message ) {
        String myEvent = message.getMessageObject();
        System.out.println( "Message received = " + myEvent );
    }
}
