package unrest.feature;

import java.io.BufferedReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import ark.util.FileUtil;
import ark.util.StringUtil;

public class UnrestFeatureUnigram extends UnrestFeature {
	private StringUtil.StringTransform cleanFn;
	private Map<String, Integer> vocabulary;
	
	public UnrestFeatureUnigram() {
		this.cleanFn = StringUtil.getDefaultCleanFn();
		this.vocabulary = null;
	}
	
	public UnrestFeatureUnigram(String vocabularyPath, int vocabularyCountThreshold) {
		this();
		this.vocabulary = new HashMap<String, Integer>();
		
		try {
			BufferedReader br = FileUtil.getFileReader(vocabularyPath);
			
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\t");
				String[] keyParts = lineParts[0].split("_");
				if (!keyParts[0].equals(getName()))
					continue;
				
				String term = keyParts[1];
				int value = Integer.parseInt(lineParts[1]);
				
				if (value < vocabularyCountThreshold)
					continue;
				
				this.vocabulary.put(term, value);
			}
			
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String getName() {
		return "unigram";
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime, String location) {
		Map<String, Integer> values = new HashMap<String, Integer>();
		String[] tokens = this.cleanFn.transform(text).split("\\s+");
		
		for (int i = 0; i < tokens.length; i++) {
			if (tokens[i].length() > 0 && (this.vocabulary == null || this.vocabulary.containsKey(tokens[i])))
				values.put(tokens[i], 1);
		}
		
		return values;
	}
	
	@Override
	public Map<String, Integer> preCompute(String text, Calendar textTime,
			String location) {
		return compute(text, textTime, location);
	}
}
