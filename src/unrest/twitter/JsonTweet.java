package unrest.twitter;


import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class for reading Tweets from their original JSON format and storing
 * information relevant for prediction.
 *  
 * @author David Bamman
 *   dbamman@cs.cmu.edu
 *
 */

public class JsonTweet {

	public static SimpleDateFormat df=new SimpleDateFormat("EEE MMM d H:m:s Z yyyy");

	long id;
	int utcOffset;
	int userId;
	String userLanguage;
	String userLocation;
	String geo;
	Date date;
	String text;
	int followersCount;
	int friendsCount;
	String name;
	String description;
	String username;
	
	public String getDescription() {
		return description;
	}
	
	public int getUserId() {
		return userId;
	}
	public JsonTweet(long id, String userLanguage, String userLocation, String geo, Date date, String text, int userId, int utcOffset, String name, String username, String description) {
		this.id=id;
		this.userLanguage=userLanguage;
		if (userLocation == null) {
			userLocation="";
		}
		if (description == null) {
			description = "";
		}
		if (name == null) {
			name="";
		}
		this.userLocation=userLocation.replaceAll("[\\r\\t\\n]", " ");
		this.geo=geo;
		this.date=date;
		this.text=text.replaceAll("[\\r\\t\\n]", " ");
		this.userId=userId;
		this.utcOffset=utcOffset;
		this.name=name.replaceAll("[\\r\\t\\n]", " ");
		this.description=description.replaceAll("[\\r\\t\\n]", " ");
		this.username=username;
	}
	public String toString() {
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", userId, userLanguage, name, username, followersCount, friendsCount, userLocation, geo, date.getTime(), utcOffset);
	}
	public JsonTweet(String[] parts) {
		
		id=Long.valueOf(parts[0]);
		userId=Integer.valueOf(parts[4]);
		userLanguage=parts[5];
		followersCount=Integer.valueOf(parts[6]);
		friendsCount=Integer.valueOf(parts[7]);
		userLocation=parts[8];
		geo=parts[9];
		date=new Date(Long.valueOf(parts[10]));
		text=parts[11];
		
	}
	public static JsonTweet readJsonTweet(String str1) {
	

		String text=null;
		Date date=null;
		String location=null;
		String geo=null;
		String userLanguage=null;
		long id=-1;
		int userId=-1;
		int followerCount=-1;
		int friendCount=-1;
		int utc_offset=-1;
		String name="";
		String username="";
		String description="";
		
		JSONObject jsonObject = null;
		
		try {
			jsonObject=JSONObject.fromObject(str1);
		} catch (Exception e1) {
			// do nothing
		}

		
		if (jsonObject == null) {
			return null;
		}
		try {
			text=(String)jsonObject.get("text");
		} catch (Exception e1) {
			// do nothing
		}
		
		if (text == null) {
			return null;
		}
		
		
		
		String dateString=(String)jsonObject.get("created_at");
		id=(Long)jsonObject.get("id");
	
		if (dateString == null) {
			return null;
		}
		
		try {
			date=df.parse(dateString);
		} catch (ParseException e) {
			// do nothing
		}
	
		try {
			JSONObject user=jsonObject.getJSONObject("user");
			if (user == null) {
				return null;
			}			
			
			try {
				userId=(Integer)user.get("id");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			
			try {
				utc_offset=(Integer)user.get("utc_offset");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			
			try {
				name=(String)user.get("name");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			
			try {
				username=(String)user.get("screen_name");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			
			try {
				description=(String)user.get("description");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			
			try {
				followerCount=(Integer)user.get("followers_count");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			try {
				friendCount=(Integer)user.get("friends_count");
			} catch (Exception e1) {
			//	System.err.println(e1);
			}
			
			try {
				userLanguage=user.getString("lang");
			} catch (Exception e1) {
				// do nothing
			}
			
			
			
			// Try to find geo information from place field
			try {
				JSONObject place=jsonObject.getJSONObject("place");
				JSONObject boundingBox=place.getJSONObject("bounding_box");
				String coord=boundingBox.getString("coordinates");
				String[] corners=coord.split("\\],\\[");
				for (int i=0; i<corners.length; i++) {
					corners[i]=corners[i].replaceAll("\\[", "");
					corners[i]=corners[i].replaceAll("\\]", "");
				}
				String[] southwest=corners[0].split(",");
				String[] northeast=corners[2].split(",");
				
				/*
				 * Twitter bounding box coordinates are 
				 * e.g., [-73.70,40.91]
				 * 
				 * UTs are:
				 * e.g., [40,01, -73.70]
				 */
				Double swLong=Double.valueOf(southwest[0]);
				Double swLat=Double.valueOf(southwest[1]);
				Double neLong=Double.valueOf(northeast[0]);
				Double neLat=Double.valueOf(northeast[1]);
				double avglat=(swLat + neLat) / 2;
				double avglong=(swLong + neLong) / 2;
				
				geo=avglat + "," + avglong;
	
			} catch (Exception e) {
				// do nothing
			}

			location=user.getString("location");
			if (location == null) {
				location="";
			}
			// Try to extract geo where the user location field is e.g. "-37.00,40.0001"
			try {
				if (location != null && location.matches(".*,.*")) {
					String[] l=location.split(",");
					double la=Double.valueOf(l[0]);
					double lo=Double.valueOf(l[1]);
					geo=la + "," + lo;
				}
			} catch (Exception e1) {
				// do nothing
			}
			
			
			if (location.startsWith("†T: ")) {
				try {
					location=location.replaceAll("†T: ", "");
					String[] parts=location.split(",");
					if (parts.length == 2) {
						double la=Double.valueOf(parts[0]);
						double lo=Double.valueOf(parts[1]);
						geo=la + "," + lo;
					}
				} catch (NumberFormatException e) {	
					// try one last time, stripping everything that's not a geo symbol from the front of the string
					try {
						String tmplocation=location.replaceAll("[^\\d.-]", "");
						String[] parts=tmplocation.split(",");
						if (parts.length == 2) {
							double la=Double.valueOf(parts[0]);
							double lo=Double.valueOf(parts[1]);
							geo=la + "," + lo;
						
						}
					} catch (NumberFormatException e1) {
						// do nothing
					}
					
				}
			}

			

		}  catch (Exception e1) {
			// do nothing
		}
		
		JsonTweet jto=new JsonTweet(id, userLanguage, location, geo, date, text, userId, utc_offset, name, username, description);
		jto.followersCount=followerCount;
		jto.friendsCount=friendCount;
		return jto;
				
	}
	public static SimpleDateFormat getDf() {
		return df;
	}
	public static void setDf(SimpleDateFormat df) {
		JsonTweet.df = df;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getUserLanguage() {
		return userLanguage;
	}
	public void setUserLanguage(String userLanguage) {
		this.userLanguage = userLanguage;
	}
	public String getUserLocation() {
		return userLocation;
	}
	public void setUserLocation(String userLocation) {
		this.userLocation = userLocation;
	}
	public String getGeo() {
		return geo;
	}
	public void setGeo(String geo) {
		this.geo = geo;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	
	public static void test(String file) {
		try {
			PrintStream out = new PrintStream(System.out, true, "UTF-8");
			BufferedReader in1 = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
			String str1;
			while ((str1 = in1.readLine()) != null) {
				String[] cols=str1.trim().split("\t");
				StringBuffer buffer=new StringBuffer();
				for (int i=0; i<4; i++) {
					buffer.append(cols[i] + "\t");
				}
				if (cols[3].equals("argentina")) {
				JsonTweet tweet=readJsonTweet(cols[5]);
				out.println(buffer.toString() + tweet);
				}
				
			}
			
			in1.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		String tweetFile=args[0];
		test(tweetFile);
	}
}
