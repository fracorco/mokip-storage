package eu.fbk.pdi.promo.simulation;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.MoreObjects;

import eu.fbk.pdi.promo.simulation.elements.Document;

public class DocInstance {

    private final Document DocType;

    private final List<Entry<String, String>> dataInstances;

    public DocInstance(final Document DocType, final List<Entry<String, String>> dataInstances) {
        super();
        this.DocType = DocType;
        this.dataInstances = dataInstances;
    }

    public Document getDocType() {
        return this.DocType;
    }

    public List<Entry<String, String>> getDataInstances() {
        return this.dataInstances;
    }

    public String getValue(final String name) {
        String res = "NOT FOUND";

        final Iterator<Entry<String, String>> ite = this.dataInstances.iterator();
        while (ite.hasNext()) {
            final Entry<String, String> varval = ite.next();
            if (varval.getKey().compareTo(name) == 0) {
                res = varval.getValue();
            }
        }
        assert res.compareTo("NOT FOUND") != 0;
        return res;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("DocType", this.DocType) //
                .add("dataInstances", this.dataInstances) //
                .toString();
    }

}
