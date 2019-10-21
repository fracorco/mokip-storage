package eu.fbk.pdi.promo.simulation.elements;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.MoreObjects;

import eu.fbk.pdi.promo.simulation.DocInstance;
import eu.fbk.pdi.promo.simulation.Randoms;

public class Document {

    private final String id;

    private final List<String> dataNames;

    private final Map<String, Boolean> dataFilled;

    private final List<Entry<String, String>> mapToReference;

    private final List<Entry<String, String[]>> randomizationValues;

    private Map<String, String> meanings;

    public Document(final String id) {
        super();
        this.id = id;
        this.dataNames = new ArrayList<>();
        this.dataFilled = new HashMap<>();
        this.mapToReference = new ArrayList<>();
        this.randomizationValues = new ArrayList<>();
    }

    public void addDataName(final String dat) {
        this.dataNames.add(dat);
        this.dataFilled.put(dat, false);
    }

    public void addAndMap1to1(final String dat) {
        this.dataNames.add(dat);
        this.dataFilled.put(dat, false);
        this.mapToReference.add(new SimpleEntry<String, String>(dat, dat));
    }

    public String getId() {
        return this.id;
    }

    public void fillFields(final String dat, final DocInstance referenceDocument) {
        this.dataFilled.put(dat, true);
        referenceDocument.getDocType().fillFields(dat);
    }

    private void fillFields(final String dat) {
        this.dataFilled.put(dat, true);
    }

    public DocInstance reconstructFromReference(final DocInstance ref) {
        final List<Entry<String, String>> datavalues = new ArrayList<Entry<String, String>>();
        final Iterator<Entry<String, String>> imap = this.mapToReference.iterator();
        while (imap.hasNext()) {
            final Entry<String, String> mapentry = imap.next();
            final String namehere = mapentry.getKey();
            final String nameinref = mapentry.getValue();
            final String valueinref = ref.getValue(nameinref);

            // Boolean filledFlag = this.dataFilled.get(namehere);
            Boolean filledFlag = ref.getDocType().dataFilled.get(namehere);
            filledFlag = true;
            if (filledFlag != null && filledFlag == true) {
                datavalues.add(new SimpleEntry<String, String>(namehere, valueinref));
            }
        }
        final DocInstance result = new DocInstance(this, datavalues);
        return result;
    }

    public DocInstance makeRandom() {
        String rstring;
        final List<Entry<String, String>> dataValues = new ArrayList<Entry<String, String>>();

        final Iterator<String> it = this.dataNames.iterator();

        while (it.hasNext()) {

            final String nxt = it.next();
            rstring = new String("");
            final Iterator<Entry<String, String[]>> ite = this.randomizationValues.iterator();
            while (ite.hasNext()) {
                final Entry<String, String[]> rnd = ite.next();
                if (rnd.getKey().compareTo(nxt) == 0) {
                    rstring = rnd.getValue()[Randoms.randInt(rnd.getValue().length)];
                }
            }

            rstring = checkIfMappedFieldExist(nxt, dataValues);
            if (rstring.compareTo("") == 0) {
                // if (nxt.startsWith("date")) {
                if (nxt.indexOf("date-") != -1) {
                    rstring = Randoms.randDate();
                } else {
                    rstring = Randoms.randUUID();
                }
            }
            dataValues.add(new SimpleEntry<String, String>(nxt, rstring));
        }
        final DocInstance res = new DocInstance(this, dataValues);
        return res;
    }

    /*
     * Used for checking if there is a field with the same "local" name, where the term "local"
     * means that the fields have the same name if the document name is removed. For compliancy
     * with the monitoring module, the two fields should have the same values in order to permit
     * the trace reconstruction. This part will be replaced by a method that will support the
     * mapping axioms for generating the same random values.
     */
    private String checkIfMappedFieldExist(final String fieldName,
            final List<Entry<String, String>> dataValues) {
        String result = "";
        final Iterator<Entry<String, String>> it = dataValues.iterator();
        while (it.hasNext()) {
            final Entry<String, String> p = it.next();
            if (p.getKey().substring(p.getKey().indexOf(".") + 1)
                    .compareTo(fieldName.substring(fieldName.indexOf(".") + 1)) == 0) {
                result = p.getValue();
                break;
            }
        }
        return result;
    }

    public boolean getFieldFillStatus(final String dat) {
        try {
            if (this.dataFilled != null) {
                return this.dataFilled.get(dat);
            }
        } catch (final Exception e) {
            System.out.println(dat);
            System.exit(0);
        }
        return false;
    }

    public Map<String, String> getMeanings() {
        return this.meanings;
    }

    public void setMeanings(final Map<String, String> meanings) {
        this.meanings = meanings;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this) //
                .add("id", this.id) //
                .add("dataNames", this.dataNames) //
                .add("dataFilled", this.dataFilled) //
                .add("mapToReference", this.mapToReference) //
                .add("randomizationValues", this.randomizationValues) //
                .add("meanings", this.meanings) //
                .toString();
    }

}
