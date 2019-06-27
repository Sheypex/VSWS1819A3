package de.tu_berlin.cit.vs.jms.common;

public class SuccessMessage extends BrokerMessage {
    Type confRegarding;

    public SuccessMessage(Type cR) {
        super(Type.SYSTEM_SUCCESS);
        confRegarding = cR;
    }

    public Type getConfRegarding() {
        return confRegarding;
    }
}
