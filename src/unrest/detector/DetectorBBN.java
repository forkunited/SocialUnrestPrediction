package unrest.detector;

import unrest.data.Gazetteer;
import unrest.util.StringUtil;
import unrest.util.UnrestProperties;

public class DetectorBBN extends DetectorConjunction {
	public DetectorBBN() {
		super();
	
		UnrestProperties properties = new UnrestProperties();
		
		Gazetteer unrestTermGazetteer = new Gazetteer("UnrestTerm", properties.getUnrestTermGazetteerPath(), StringUtil.getDefaultCleanFn());
		//Gazetteer unrestLocationGazetteer = new Gazetteer("UnrestLocation", properties.getUnrestLocationGazetteerPath(), StringUtil.getDefaultCleanFn());
		
		this.detectors.add(new DetectorGazetteer(unrestTermGazetteer, false));
		//this.detectors.add(new DetectorGazetteer(unrestLocationGazetteer, true));
		//this.detectors.add(new DetectorFutureDate());
	}
}
