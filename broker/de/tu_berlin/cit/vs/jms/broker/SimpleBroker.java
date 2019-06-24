package de.tu_berlin.cit.vs.jms.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;

import de.tu_berlin.cit.vs.jms.common.*;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;


public class SimpleBroker {
    private class StockTopic {
        Topic topic;
        MessageProducer prod;

        public StockTopic(Topic t) {
            topic = t;
            try {
                prod = topicSession.createProducer(topic);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public void send(Stock s) {
            try {
                prod.send(topicSession.createObjectMessage(s));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public String name() {
            try {
                return topic.getTopicName();
            } catch (JMSException e) {
                e.printStackTrace();
            }
            return "";
        }
    }

    private class ClientHandle {
        Queue inputQ;
        MessageConsumer inputQCons;
        Queue outputQ;
        MessageProducer outputQProd;
        String name;
        Session session;
        ClientHandle me;

        private final MessageListener clientListener = new MessageListener() {
            private void onUnregMsg(UnregisterMessage msg) {
                clientList.remove(me);
                return;
            }

            private void onListMsg(ListMessage msg) {
                try {
                    synchronized (stockList) {
                        outputQProd.send(session.createObjectMessage(new ListMessage(stockList)));
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                return;
            }

            private void onBuyMsg(BuyMessage msg) {
                try {
                    if (buy(msg.getStockName(), msg.getAmount()) == -1) {
                        send(new ErrorMessage());
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                return;
            }

            private void onSellMsg(SellMessage msg) {
                try {
                    if (sell(msg.getStockName(), msg.getAmount()) == -1) {

                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                return;
            }

            @Override
            public void onMessage(Message msg) {
                if (msg instanceof ObjectMessage) {
                    try {
                        BrokerMessage brokMsg = (BrokerMessage) ((ObjectMessage) msg).getObject();
                        switch (brokMsg.getType()) {
                            case SYSTEM_UNREGISTER:
                                UnregisterMessage unregMsg = (UnregisterMessage) brokMsg;
                                onUnregMsg(unregMsg);
                                break;
                            case STOCK_LIST:
                                ListMessage listMsg = (ListMessage) brokMsg;
                                onListMsg(listMsg);
                                break;
                            case STOCK_BUY:
                                BuyMessage buyMsg = (BuyMessage) brokMsg;
                                onBuyMsg(buyMsg);
                                break;
                            case STOCK_SELL:
                                SellMessage sellMsg = (SellMessage) brokMsg;
                                onSellMsg(sellMsg);
                                break;
                            default:
                                Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.SEVERE, "unsupported BrokerMessage type");
                                break;
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        public ClientHandle(String n) {
            name = n;
            me = this;
            try {
                session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (JMSException e) {
                e.printStackTrace();
            }
            try {
                inputQ = session.createQueue(name + "_input");
                inputQCons = session.createConsumer(inputQ);
                inputQCons.setMessageListener(clientListener);
                outputQ = session.createQueue(name + "_output");
                outputQProd = session.createProducer(outputQ);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public void send(BrokerMessage msg) {
            try {
                outputQProd.send(session.createObjectMessage(msg));
            } catch (JMSException e) {
                e.printStackTrace();
            }
            return;
        }

    }

    private final MessageListener reqQlistener = new MessageListener() {
        private void onRegMsg(RegisterMessage msg) {
            String name = msg.getClientName();
            clientList.add(new ClientHandle(name));
            return;
        }

        @Override
        public void onMessage(Message msg) {
            if (msg instanceof ObjectMessage) {
                try {
                    BrokerMessage brokMsg = (BrokerMessage) ((ObjectMessage) msg).getObject();
                    switch (brokMsg.getType()) {
                        case SYSTEM_REGISTER:
                            RegisterMessage regMsg = (RegisterMessage) brokMsg;
                            onRegMsg(regMsg);
                            break;
                        default:
                            Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.SEVERE, "unsupported BrokerMessage type");
                            break;
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    };


    /* TODO: variables as needed */
    List<StockTopic> stockTopicList;
    List<Stock> stockList;
    List<ClientHandle> clientList;
    Connection con;
    Session topicSession;

    public SimpleBroker(List<Stock> stockList) throws JMSException {
        /* TODO: initialize connection, sessions, etc. */

        ActiveMQConnectionFactory conFac = new ActiveMQConnectionFactory("tcp://localhost:61616");
        con = conFac.createConnection();
        con.start();

        topicSession = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

        stockTopicList = new ArrayList<>(stockList.size());
        this.stockList = stockList;
        for (Stock stock : stockList) {
            /* TODO: prepare stocks as topics */
            Topic nT = topicSession.createTopic(stock.getName());
            stockTopicList.add(new StockTopic(nT));
        }

        clientList = new ArrayList<>();

        Session regSession = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue regQ = regSession.createQueue("registration");
        MessageConsumer regQCons = regSession.createConsumer(regQ);
        regQCons.setMessageListener(reqQlistener);
    }

    public void stop() throws JMSException {
        //TODO
        con.stop();
    }

    public synchronized int buy(String stockName, int amount) throws JMSException {
        Stock stock = null;
        for (Stock s : stockList) {
            if (s.getName().equals(stockName)) {
                stock = s;
                break;
            }
        }
        if (stock == null) {
            return -1;
        }
        if (amount > stock.getAvailableCount()) {
            return -1;
        }
        stock.setAvailableCount(stock.getAvailableCount() - amount);
        //TODO
        StockTopic stockTopic = null;
        for (StockTopic st : stockTopicList) {
            if (st.name().equals(stockName)) {
                stockTopic = st;
                break;
            }
        }
        if (stockTopic == null) {
            return -1;
        }
        stockTopic.send(stock);
        return 0;
    }

    public synchronized int sell(String stockName, int amount) throws JMSException {
        Stock stock = null;
        for (Stock s : stockList) {
            if (s.getName().equals(stockName)) {
                stock = s;
                break;
            }
        }
        if (stock == null) {
            return -1;
        }
        stock.setAvailableCount(stock.getAvailableCount() + amount);
        //TODO
        StockTopic stockTopic = null;
        for (StockTopic st : stockTopicList) {
            if (st.name().equals(stockName)) {
                stockTopic = st;
                break;
            }
        }
        if (stockTopic == null) {
            return -1;
        }
        stockTopic.send(stock);
        return 0;
    }

    /*public synchronized List<Stock> getStockList() {
        List<Stock> stockList = new ArrayList<>();

        *//* TODO: populate stockList *//*

        return stockList;
    }*/
}
