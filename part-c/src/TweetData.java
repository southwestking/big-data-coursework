import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;

public class TweetData implements WritableComparable<TweetData> {
	private int CHARACTER_LIMIT = 140;

	private Text id;
	private Text dateTime;
	private Text hashtages;
	private Text tweet;

	private boolean hasHashtages;
	private IntWritable tweetLenght;

	public TweetData(){
		
	}
	
	/**
		0 = tweet ID
		1 = data/time
		2 = hashtages
		3 = tweet
	*/
	public void setFromString(String value){
		String[] tweetData = value.split(";");
		set(tweetData[0],tweetData[1],tweetData[2],tweetData[3]);
	}
	
	public void set(String id, String dateTime, String hashtages, String tweet){
		this.id = new Text(id);
		this.dateTime = new Text(dateTime);
		this.hashtages = new Text(hashtages);	
		this.tweet = new Text(tweet);
	}
	
	public Text getId(){ 
		return id;
	}
	public Text getDateTime(){ 
		return dateTime;
	}
	public Text getHashtages(){ 
		return hashtages;
	}
	public Text getTweet(){ 
		return tweet;
	}
	public Text getDate(){ 
		String[] dates = getDateTime().toString().split(",");
		return new Text(dates[0]);
	}
	
	public int getTweetLenght(){ 
		return getTweet().toString().length();
	}
	
	public StringTokenizer getStringTokenizer(){
		StringTokenizer itr = new StringTokenizer(tweet.toString(), " \t\n\r\f,.:;?!#[]'");
		return itr;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		dateTime.readFields(in);
		hashtages.readFields(in);
		tweet.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		dateTime.write(out);
		hashtages.write(out);
		tweet.write(out);
	}
	
	
	public int compareTo(TweetData td/*DailyStock st*/) {
		// int cmp = company.compareTo(st.getCompany());
		// if (cmp != 0) {
		// 	return cmp;
		// }
		// return day.compareTo(st.getDay());
		return 0;
	}
}
