package eu.fbk.pdi.promo.rdf4j.functions;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

public class TimestampFunction implements Function {

	@Override
	public String getURI() {
		return "http://shell.fbk.eu/custom_functions/timestamp";
	}

	@Override
	public Value evaluate(ValueFactory vf, Value... args) throws ValueExprEvaluationException {
		Literal literal = (Literal) args[0];
		XMLGregorianCalendar calendar = literal.calendarValue();
		long timestamp = calendar.toGregorianCalendar().getTime().getTime();
		return vf.createLiteral(timestamp);
	}

}
