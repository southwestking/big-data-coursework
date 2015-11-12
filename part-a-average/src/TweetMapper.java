import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    
	//key has to be the same for the count 
	private final Text one = new Text("1");
	TweetData tweetObj = new TweetData();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
		String[] tweetData = value.toString().split(";");
		
		tweetObj.setFromString(value.toString());
		int tweetLength = tweetObj.getTweetLenght();
		if(tweetLength > 0 && tweetLength < 141)
			context.write(one, new IntWritable(tweetLength));	        
    }
	
	public String minMax(int value){
	//this keeps it in the 1-5,6-10 range
		value = (value % 5 == 0) ? value-1 : value; 
		if (value<=5)
			return (1 + "-" + 5);

		int from = value - value % 5 + 1;
		int to = from + 4;
		return (from + "-" + to);

	}
}
