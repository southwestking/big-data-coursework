import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    
	private final IntWritable one = new IntWritable(1);
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  
		int tweetLength = getTweet(value).length();
		if(tweetLength > 0 && tweetLength < 141)
			context.write(new Text(minMax(tweetLength)), one);	        
    }

	public String getTweet(Text txt){
     	String dump = txt.toString();
		if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
        		int startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
        		return dump.substring(startIndex,dump.lastIndexOf(';'));
		}
		return "";
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
