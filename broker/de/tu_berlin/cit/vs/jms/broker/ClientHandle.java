package de.tu_berlin.cit.vs.jms.broker;

import de.tu_berlin.cit.vs.jms.common.*;

import javax.jms.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientHandle extends Thread {
    Queue inputQ;
    MessageConsumer inputQCons;
    Queue outputQ;
    MessageProducer outputQProd;
    String name;
    Session session;
    SimpleBroker server;
    boolean running;

    private void onUnregMsg(UnregisterMessage msg) {
        synchronized (server.clientList) {
            server.clientList.remove(this);
        }
        send(new SuccessMessage(BrokerMessage.Type.SYSTEM_UNREGISTER));
        Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, "Successfully unregistered " + name);
        die();
        return;
    }

    private void onRequestListMsg(RequestListMessage msg) {
        synchronized (server.stockList) {
            send(new ListMessage(server.stockList));
            send(new SuccessMessage(BrokerMessage.Type.STOCK_LIST));
        }
        Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, "Successfully sent stock list to " + name);
        return;
    }

    private void onBuyMsg(BuyMessage msg) {
        try {
            Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, "Trying to buy " + msg.getAmount() + " units of stock " + msg.getStockName() + " for " + name);
            if (server.buy(msg.getStockName(), msg.getAmount()) == -1) {
                send(new ErrorMessage(BrokerMessage.Type.STOCK_BUY));
                Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, "Encountered an error buying " + msg.getAmount() + " units of stock " + msg.getStockName());
            } else {
                send(new SuccessMessage(BrokerMessage.Type.STOCK_BUY));
                Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, name + " successfully bought " + msg.getAmount() + " units of stock " + msg.getStockName());
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return;
    }

    private void onSellMsg(SellMessage msg) {
        try {
            Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, "Trying to sell " + msg.getAmount() + " units of stock " + msg.getStockName() + " for " + name);
            if (server.sell(msg.getStockName(), msg.getAmount()) == -1) {
                send(new ErrorMessage(BrokerMessage.Type.STOCK_SELL));
                Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, "Encountered an error selling " + msg.getAmount() + "units of stock " + msg.getStockName());
            } else {
                send(new SuccessMessage(BrokerMessage.Type.STOCK_SELL));
                Logger.getLogger(JmsBrokerServer.class.getName()).log(Level.INFO, name + "successfully sold " + msg.getAmount() + " units of stock " + msg.getStockName());
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return;
    }

    public ClientHandle(String n, SimpleBroker sB) {
        running = true;
        server = sB;
        name = n;
        try {
            session = server.con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        try {
            inputQ = session.createQueue(name + "_input");
            inputQCons = session.createConsumer(inputQ);
            outputQ = session.createQueue(name + "_output");
            outputQProd = session.createProducer(outputQ);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void die() {
        try {
            session.close();
            running = false;
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

    @Override
    public void run() {
        while (running) {
            try {
                Message msg = inputQCons.receive(1000);
                if (msg instanceof ObjectMessage) {
                    try {
                        BrokerMessage brokMsg = (BrokerMessage) (((ObjectMessage) msg).getObject());
                        switch (brokMsg.getType()) {
                            case SYSTEM_UNREGISTER:
                                UnregisterMessage unregMsg = (UnregisterMessage) brokMsg;
                                onUnregMsg(unregMsg);
                                break;
                            case STOCK_LIST:
                                RequestListMessage requestListMsg = (RequestListMessage) brokMsg;
                                onRequestListMsg(requestListMsg);
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
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
