package eu.fbk.pdi.promo.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;

public final class Constants {

    public static final String DEFAULT_TRACE_NAMESPACE = "trace:";

    public static final String DEFAULT_GRAPH_NAMESPACE = "promo:";

    public static final Path DEFAULT_ONTOLOGY_FILE = Paths.get("models/ontology.tql.gz");

    public static final Path DEFAULT_DIAGRAM_FILE = Paths.get("models/diagram.json");

    public static final Path DEFAULT_TRACES_FILE = Paths.get("traces/traces.jsonl.gz");

    public static final int DEFAULT_NUM_TRACES = 100;

    public static final int DEFAULT_STORE_THREADS = 4;

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
            "dd-MM-yyyy'T'HH:mm:ssZ");

    public static final int PARTIAL_TRACES_BATCH = 5;

    private Constants() {
        throw new Error();
    }

}
