import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;

public class TweetData implements WritableComparable<TweetData> {

	private Text id;
	private Longwritable dateTime;
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
		this.id.set(id);
		this.dateTime.set(dateTime);
		this.hashtages.set(hashtages);
		this.tweet.set(tweet);
	}
	
	public Text getId(){ return (id!=null) ? id : null;}
	public LongWritable getDateTime(){ return (dateTime!=null) ? dateTime : null;}
	public Text getHashtages(){ return (hashtages!=null) ? hashtages : null;}
	public Text getTweet(){ return (tweet!=null) ? tweet : null;}
	
	public LongWritable getDate(){
		new SimpleDateFormat("yyyy-MM-dd").format(new Date(get()) 
		return (dateTime!=null) ? dateTime : null;
	}
	public Text getTweetLenght(){ return (id!=null) ? id : null;}
	public Text getLimiterString(){ return (id!=null) ? id : null;}
	
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
