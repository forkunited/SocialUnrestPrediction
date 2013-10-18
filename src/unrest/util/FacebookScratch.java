package unrest.util;

import java.util.List;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.DefaultJsonMapper;
import com.restfb.FacebookClient;
import com.restfb.JsonMapper;
import com.restfb.types.NamedFacebookType;
import com.restfb.types.Page;
import com.restfb.types.Post;
import com.restfb.types.Post.Comments;
import com.restfb.types.Post.Likes;

public class FacebookScratch {
	public static void main(String[] args) {
		String pageId = "https://www.facebook.com/www.cstsconfederacion.org"; // 165192246864611
		FacebookClient client = new DefaultFacebookClient("CAACEdEose0cBAGmuTvFkxzXDKbyGAtCgCa0dbwRlcS7Hw0p8ZBBFxJxVVM3dGD4WEUzpmU9R3jhI02hMFbeixJzm1PZCIBm2zaIZC3W58QE82Oyo1QhLSUVmHwUH4D3kmhnbmlAXZAkFLZBgF2zKDCbUXvZA0WzSV2W74Gt1A2iEDVUgc8zDnFt9ZCJMOWpvHYZD");
		Page page = client.fetchObject(pageId, Page.class);
		System.out.println(page.getId());
		//Connection<Post> feed = client.fetchConnection(pageId + "/feed", Post.class);
		//JsonMapper jsonMapper = new DefaultJsonMapper();
		//System.out.println(page.getName());
		/*for (List<Post> feedPage : feed) {
			for (Post post : feedPage) {
				System.out.println(post.toString());
				//post.getComments().getData().get(0).getFrom().getId()
				post.get
				Comments comments = post.getComments();
				Likes likes = post.getLikes();
				List<NamedFacebookType> likeData = likes.getData();
				likeData.get(0).toString();
				break;
			}
			break;
		}*/
		// For each page want:
		//	[id]/likes
		//	[id]/feed
			// For each post, get comments and likes.  For each comment, get user
		//	[id]/events
	}
}