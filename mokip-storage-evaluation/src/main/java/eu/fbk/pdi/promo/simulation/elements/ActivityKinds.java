package eu.fbk.pdi.promo.simulation.elements;

public enum ActivityKinds {

    TASK, COL_SUBPROC, COL_EVENT, EXP_SUBPROC, EXP_EVENT, // Activities

    XOR, EVENT, PARALLEL, INCLUSIVE, COMPLEX, // Gateways

    JOIN, PARALLEL_JOIN, INCLUSIVE_JOIN, COMPLEX_JOIN, // Joining Gateways

    START, START_MSG, // Starts (to be completed)

    END, END_MSG, // Ends (to be completed)

    INTERMEDIATE_MSG, INTERMEDIATE_TIMER, // Catch/throws (to be completed)

    CATCH_INTERMEDIATE_MSG, CATCH_INTERMEDIATE_TIMER, // Catch/throws (to be completed)

    THROW_INTERMEDIATE_MSG

}
