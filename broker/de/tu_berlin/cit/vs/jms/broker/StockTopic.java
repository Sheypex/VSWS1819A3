package de.tu_berlin.cit.vs.jms.broker;

import de.tu_berlin.cit.vs.jms.common.Stock;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

public class StockTopic extends Thread{
    Topic topic;
    Stock stock;
    MessageProducer prod;
    SimpleBroker server;
    Session session;
    boolean running;
    Boolean sendUpdate;
    private final Object lock = new Object();

    public StockTopic(Stock s, SimpleBroker sB) {
        running = true;
        sendUpdate = false;
        server = sB;
        try {
            session = server.con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            topic = session.createTopic(s.getName());
        } catch (JMSException e) {
            e.printStackTrace();
        }
        stock = s;
        try {
            prod = session.createProducer(topic);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void send(Stock s) {
        try {
            synchronized (stock) {
                prod.send(session.createObjectMessage(s));
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void send() {
        send(stock);
    }

    public Stock getStock() {
        return stock;
    }

    public String name() {
        return stock.getName();
    }

    public synchronized void updateMessage() {
        synchronized (lock) {
            sendUpdate = true;
            lock.notify();
        }
    }

    public void die(){
        running = false;
        try {
            session.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (running) {
            synchronized (lock) {
                while (!sendUpdate) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                send();
                sendUpdate = false;
            }
        }
    }
}
