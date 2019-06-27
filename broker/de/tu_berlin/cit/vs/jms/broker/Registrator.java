package de.tu_berlin.cit.vs.jms.broker;

import de.tu_berlin.cit.vs.jms.common.BrokerMessage;
import de.tu_berlin.cit.vs.jms.common.RegisterMessage;
import de.tu_berlin.cit.vs.jms.common.SuccessMessage;

import javax.jms.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Registrator extends Thread {
    private void onRegMsg(RegisterMessage msg) {
        String name = msg.getClientName();
        ClientHandle nC = new ClientHandle(name, server);
        synchronized (server.clientList) {
            server.clientList.add(nC);
        }
        nC.send(new SuccessMessage(BrokerMessage.Type.SYSTEM_REGISTER));
        return;
    }

    Session session;
    SimpleBroker server;
    Queue regQ;
    MessageConsumer regQCons;
    boolean running;

    public Registrator(SimpleBroker sB) {
        running = true;
        server = sB;
        try {
            session = server.con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            regQ = session.createQueue("registration");
            regQCons = session.createConsumer(regQ);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void die() {
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
            try {
                Message msg = regQCons.receive(1000);
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
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
