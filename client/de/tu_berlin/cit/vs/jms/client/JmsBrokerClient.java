package de.tu_berlin.cit.vs.jms.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;

import de.tu_berlin.cit.vs.jms.common.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;


public class JmsBrokerClient {
    private String clientName;
    Connection con;
    Session session;
    Queue outputQ;
    MessageConsumer outputQCons;
    Queue inputQ;
    MessageProducer inputQProd;
    List<Stock> stockList;
    Boolean awaitingConf;
    BrokerMessage.Type confType;
    private final Object lock1 = new Object();
    private final Object lock2 = new Object();

    private boolean awaitingConf() {
        synchronized (lock1) {
            while (awaitingConf) {
                try {
                    lock1.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return awaitingConf;
        }
    }

    private void awaitConf(BrokerMessage.Type type) {
        synchronized (lock2) {
            awaitingConf = true;
            confType = type;
            synchronized (lock1) {
                lock1.notify();
            }
        }
    }

    private void resolveConf(BrokerMessage.Type type) {
        synchronized (lock2) {
            if (confType == type) {
                awaitingConf = false;
                synchronized (lock1) {
                    lock1.notify();
                }
            } else {
                throw new RuntimeException("Messed up confirmation order");
            }
        }
    }

    MessageListener outputQListener = new MessageListener() {
        public void onListMsg(ListMessage msg) {
            stockList = msg.getStocks();
        }

        @Override
        public void onMessage(Message msg) {
            if (msg instanceof ObjectMessage) {
                try {
                    BrokerMessage brokMsg = (BrokerMessage) (((ObjectMessage) msg).getObject());
                    switch (brokMsg.getType()) {
                        case STOCK_LIST:
                            ListMessage listMsg = (ListMessage) brokMsg;
                            onListMsg(listMsg);
                            break;
                        case SYSTEM_SUCCESS:
                            SuccessMessage succMsg = (SuccessMessage) brokMsg;
                            System.out.print("Successfully ");
                            switch (succMsg.getConfRegarding()) {
                                case STOCK_BUY:
                                    System.out.println("bought");
                                    break;
                                case SYSTEM_REGISTER:
                                    System.out.println("registered");
                                    break;
                                case STOCK_LIST:
                                    System.out.println("received stock list");
                                    break;
                                case STOCK_SELL:
                                    System.out.println("sold");
                                    break;
                                case SYSTEM_UNREGISTER:
                                    System.out.println("unregistered");
                                    break;
                                default:
                                    Logger.getLogger(JmsBrokerClient.class.getName()).log(Level.SEVERE, "unsupported confRegarding Type");
                                    break;
                            }
                            resolveConf(succMsg.getConfRegarding());
                            break;
                        case SYSTEM_ERROR:
                            ErrorMessage errMsg = (ErrorMessage) brokMsg;
                            System.out.print("Server encountered an error ");
                            switch (errMsg.getErrorRegarding()) {
                                case SYSTEM_REGISTER:
                                    System.out.println("registering");
                                    throw new RuntimeException("Couldn't register");
                                case STOCK_BUY:
                                    System.out.println("buying");
                                    break;
                                case STOCK_LIST:
                                    System.out.println("providing the stock list");
                                    throw new RuntimeException("No stock list available");
                                case STOCK_SELL:
                                    System.out.println("selling");
                                    break;
                                case SYSTEM_UNREGISTER:
                                    System.out.println("unregistering");
                                    break;
                            }
                            resolveConf(errMsg.getErrorRegarding());
                            break;
                        default:
                            Logger.getLogger(JmsBrokerClient.class.getName()).log(Level.SEVERE, "unsupported BrokerMessage Type");
                            break;
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    public JmsBrokerClient(String clientName) throws JMSException {
        this.clientName = clientName;
        awaitingConf = false;

        /* TODO: initialize connection, sessions, consumer, producer, etc. */
        ActiveMQConnectionFactory conFac = new ActiveMQConnectionFactory("tcp://localhost:61616");
        conFac.setTrustAllPackages(true);
        con = conFac.createConnection();
        con.start();

        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        start();
    }

    public void start() {
        stockList = new ArrayList<>();
        try {
            Queue regQ = session.createQueue("registration");
            MessageProducer regQProd = session.createProducer(regQ);
            awaitConf(BrokerMessage.Type.SYSTEM_REGISTER);
            regQProd.send(session.createObjectMessage(new RegisterMessage(clientName)));
        } catch (JMSException e) {
            e.printStackTrace();
        }
        // TODO add handshake to ensure registration went well(?)
        try {
            inputQ = session.createQueue(clientName + "_input");
            inputQProd = session.createProducer(inputQ);
            outputQ = session.createQueue(clientName + "_output");
            outputQCons = session.createConsumer(outputQ);
            outputQCons.setMessageListener(outputQListener);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        try {
            requestList();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        System.out.println("Registered on server and received stock list:");
        for (Stock s : stockList) {
            System.out.println(s.toString());
        }
    }

    public void send(BrokerMessage brokerMessage) {
        try {
            awaitingConf();

            awaitConf(brokerMessage.getType());
            inputQProd.send(session.createObjectMessage(brokerMessage));
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void requestList() throws JMSException {
        send(new RequestListMessage());
    }

    public void buy(String stockName, int amount) throws JMSException {
        send(new BuyMessage(stockName, amount));
    }

    public void sell(String stockName, int amount) throws JMSException {
        send(new SellMessage(stockName, amount));
    }

    public void watch(String stockName) throws JMSException {
        //TODO
    }

    public void unwatch(String stockName) throws JMSException {
        //TODO
    }

    public void quit() throws JMSException {
        awaitConf(BrokerMessage.Type.SYSTEM_UNREGISTER);
        send(new UnregisterMessage(clientName));
        while (!awaitingConf()) {
        }
        session.close();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter the client name:");
            String clientName = reader.readLine();

            JmsBrokerClient client = new JmsBrokerClient(clientName);

            boolean running = true;
            while (running) {
                System.out.println("Enter command:");
                String[] task = reader.readLine().split(" ");

                synchronized (client) {
                    switch (task[0].toLowerCase()) {
                        case "quit":
                            client.quit();
                            System.out.println("Bye bye");
                            running = false;
                            break;
                        case "list":
                            client.requestList();
                            break;
                        case "buy":
                            if (task.length == 3) {
                                client.buy(task[1], Integer.parseInt(task[2]));
                            } else {
                                System.out.println("Correct usage: buy [stock] [amount]");
                            }
                            break;
                        case "sell":
                            if (task.length == 3) {
                                client.sell(task[1], Integer.parseInt(task[2]));
                            } else {
                                System.out.println("Correct usage: sell [stock] [amount]");
                            }
                            break;
                        case "watch":
                            if (task.length == 2) {
                                client.watch(task[1]);
                            } else {
                                System.out.println("Correct usage: watch [stock]");
                            }
                            break;
                        case "unwatch":
                            if (task.length == 2) {
                                client.unwatch(task[1]);
                            } else {
                                System.out.println("Correct usage: unwatch [stock]");
                            }
                            break;
                        default:
                            System.out.println("Unknown command. Try one of:");
                            System.out.println("quit, list, buy, sell, watch, unwatch");
                    }
                }
            }

        } catch (JMSException | IOException ex) {
            Logger.getLogger(JmsBrokerClient.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
