package eu.fbk.pdi.promo.simulation.elements;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import eu.fbk.pdi.promo.simulation.DocInstance;

public class Emitter {

    private final String docId;

    private final String docrole;

    private List<Entry<String, String>> outputMap;

    public Emitter(final String docId, final String docrole) {
        super();
        this.docId = docId;
        this.docrole = docrole;
        this.outputMap = new ArrayList<Entry<String, String>>();
    }

    public void addToOutputMap(final String from, final String to) {
        this.outputMap = Emitter.insertPair(this.outputMap,
                new SimpleEntry<String, String>(from, to));
    }

    public String getDocId() {
        return this.docId;
    }

    public String getDocrole() {
        return this.docrole;
    }

    public List<Entry<String, String>> getOutputMap() {
        return this.outputMap;
    }

    public List<Entry<String, String>> generateValues_noDefault(final DocInstance d,
            final DocInstance referenceDocument) {
        List<Entry<String, String>> res = new ArrayList<Entry<String, String>>();
        assert d.getDocType().getId().compareTo(this.docId) == 0;
        final List<Entry<String, String>> varvals = d.getDataInstances();
        final Iterator<Entry<String, String>> ivarvals = varvals.iterator();
        while (ivarvals.hasNext()) {
            final Entry<String, String> varval = ivarvals.next();
            boolean found = false;
            String vname = new String("");
            final String val = varval.getValue();
            final Iterator<Entry<String, String>> imap = this.outputMap.iterator();
            while (imap.hasNext()) {
                final Entry<String, String> onemap = imap.next();
                if (varval.getKey().compareTo(onemap.getKey()) == 0) {
                    found = true;
                    vname = onemap.getValue();
                }
            }
            if (found) {
                if (referenceDocument.getDocType().getFieldFillStatus(vname)) {
                    res = Emitter.insertPair(res,
                            new SimpleEntry<String, String>(Emitter.stripType(vname), val));
                }
            } else {
                if (referenceDocument.getDocType().getFieldFillStatus(varval.getKey())) {
                    res = Emitter.insertPair(res, new SimpleEntry<String, String>(
                            Emitter.stripType(varval.getKey()), val));
                }
            }
        }
        return res;
    }

    private static String stripType(final String s) {
        if (s.startsWith("date_")) {
            return s.substring(5);
        }
        return s;
    }

    private static List<Entry<String, String>> insertPair(final List<Entry<String, String>> l,
            final Entry<String, String> p) {
        final List<Entry<String, String>> res = new ArrayList<Entry<String, String>>();
        Iterator<Entry<String, String>> i;

        i = l.iterator();
        while (i.hasNext()) {
            final Entry<String, String> p1 = i.next();
            if (p.getKey().compareTo(p1.getKey()) > 0) {
                res.add(p1);
            }
        }
        res.add(p);
        i = l.iterator();
        while (i.hasNext()) {
            final Entry<String, String> p1 = i.next();
            if (p.getKey().compareTo(p1.getKey()) < 0) {
                res.add(p1);
            }
        }
        return res;
    }

}
