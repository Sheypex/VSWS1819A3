package de.tu_berlin.cit.vs.jms.client;

import de.tu_berlin.cit.vs.jms.common.Stock;

import javax.jms.*;

public class TopicWatch {
    Topic topic;
    MessageConsumer topicCons;

    MessageListener topicListener = new MessageListener() {
        @Override
        public void onMessage(Message msg) {
            if (msg instanceof ObjectMessage){
                try {
                    Stock s = (Stock)(((ObjectMessage) msg).getObject());
                    System.out.println("Update to stock " + s.getName() + ":");
                    System.out.println(s.toString());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    public TopicWatch(String stockName, Session session) {
        try {
            topic = session.createTopic(stockName);
            topicCons = session.createConsumer(topic);
            topicCons.setMessageListener(topicListener);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void die(){
        try {
            topicCons.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
