package unrest.util;

public class StringUtil {
	/* String cleaning and measure helpers... maybe move these to separate classes later. */
	public interface StringPairMeasure {
		double compute(String str1, String str2);
	}
	
	public interface StringTransform {
		String transform(String str);
		String toString(); // Return constant name for this transformation (used in feature names)
	}
	
	public static StringUtil.StringTransform getDefaultCleanFn() {
		return new StringUtil.StringTransform() {
			public String toString() {
				return "DefaultClean";
			}
			
			public String transform(String str) {
				return StringUtil.clean(str);
			}
		};	
	}
	
	// FIXME: This function is messy and inefficient.
	public static String clean(String str) {
		StringBuilder cleanStrBuilder = new StringBuilder();
		
		str = str.trim();
		if (str.equals("$") || str.equals("&") || str.equals("+") || str.equals("@"))
			return str;
		
		// Remove words with slashes
		String[] tokens = str.split("\\s+");
		for (int i = 0; i < tokens.length; i++) {
			if (!tokens[i].startsWith("/") && !tokens[i].startsWith("\\") && !tokens[i].startsWith("-"))
				cleanStrBuilder.append(tokens[i]).append(" ");
		}
		
		// Remove non alpha-numeric characters
		String cleanStr = cleanStrBuilder.toString();
		cleanStr = cleanStr.toLowerCase()
						   .replaceAll("[\\W&&[^\\s]]+", "")
						   .replaceAll("\\s+", " ");
		
		return cleanStr.trim();
	}
}