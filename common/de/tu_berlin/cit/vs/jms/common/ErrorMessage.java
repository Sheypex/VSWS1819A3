package de.tu_berlin.cit.vs.jms.common;

public class ErrorMessage extends BrokerMessage {
    public ErrorMessage() {
        super(Type.SYSTEM_ERROR);
    }
}
