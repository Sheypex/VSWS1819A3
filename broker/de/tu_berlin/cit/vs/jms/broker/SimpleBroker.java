package de.tu_berlin.cit.vs.jms.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;

import de.tu_berlin.cit.vs.jms.common.*;
import org.apache.activemq.ActiveMQConnectionFactory;


public class SimpleBroker {
    /* TODO: variables as needed */
    List<StockTopic> stockTopicList;
    List<Stock> stockList;
    List<ClientHandle> clientList;
    Connection con;
    Registrator reg;

    public SimpleBroker(List<Stock> stockList) throws JMSException {
        /* TODO: initialize connection, sessions, etc. */

        ActiveMQConnectionFactory conFac = new ActiveMQConnectionFactory("tcp://localhost:61616");
        conFac.setTrustAllPackages(true);
        con = conFac.createConnection();
        con.start();

        stockTopicList = new ArrayList<>(stockList.size());
        this.stockList = stockList;
        for (Stock stock : stockList) {
            /* TODO: prepare stocks as topics */
            StockTopic nST = new StockTopic(stock, this);
            stockTopicList.add(nST);
            nST.start();
            nST.setName(stock.getName() + " StockTopic Thread");
        }

        clientList = new ArrayList<>();

        reg = new Registrator(this);
        reg.start();
        reg.setName("Registrator Thread");
    }

    public void stop() throws JMSException {
        //TODO
        reg.die();
        for (StockTopic sT : stockTopicList){
            sT.die();
        }
        for (ClientHandle c : clientList) {
            c.die();
        }
        con.stop();
    }

    public synchronized int buy(String stockName, int amount) throws JMSException {
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
        Stock stock = stockTopic.getStock();
        if (stock == null) {
            return -1;
        }
        if (amount > stock.getAvailableCount()) {
            return -1;
        }
        synchronized (stock) {
            stock.setAvailableCount(stock.getAvailableCount() - amount);
        }
        stockTopic.updateMessage();
        return 0;
    }

    public synchronized int sell(String stockName, int amount) throws JMSException {
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
        Stock stock = stockTopic.getStock();
        if (stock == null) {
            return -1;
        }
        synchronized (stock) {
            stock.setAvailableCount(stock.getAvailableCount() + amount);
        }
        stockTopic.updateMessage();
        return 0;
    }

    /*public synchronized List<Stock> getStockList() {
        List<Stock> stockList = new ArrayList<>();

        *//* TODO: populate stockList *//*

        return stockList;
    }*/
}
