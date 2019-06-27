package de.tu_berlin.cit.vs.jms.common;

public class ErrorMessage extends BrokerMessage {
    Type errorRegarding;

    public ErrorMessage(Type eR) {
        super(Type.SYSTEM_ERROR);
        errorRegarding = eR;
    }

    public Type getErrorRegarding() {
        return errorRegarding;
    }
}
