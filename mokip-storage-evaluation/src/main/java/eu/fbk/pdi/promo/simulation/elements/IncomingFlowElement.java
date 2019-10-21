package eu.fbk.pdi.promo.simulation.elements;

import java.util.List;

public class IncomingFlowElement {

    private List<String> preconditions;

    public IncomingFlowElement(List<String> conditions) {
        preconditions = conditions;
    }

    public List<String> getPreconditions() {
        return preconditions;
    }

}
