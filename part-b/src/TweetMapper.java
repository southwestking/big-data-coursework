import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();
	TweetData tweetObj = new TweetData();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // while (itr.hasMoreTokens()) {
        //   data.set(itr.nextToken().toLowerCase());
        //   context.write(data, one);
		//}
		//String[] tweetData = value.toString().split(";");
		
		tweetObj.setFromString(value.toString());
		StringTokenizer itr = tweetObj.getStringTokenizer();
				
		while (itr.hasMoreTokens()) {
			//int rounds down for 1-5...
			int tweetLength = itr.nextToken().length();
			
			data.set(minMax(tweetLength));
			context.write(data, one);
        }
    }
	
	public String minMax(int value){
	//this keeps it in the 1-5,6-10 range
		value = (value % 5 == 0) ? value-1 : value; 
			
		if (value<=0)
			return (1 + "-" + 5);

		int from = value - value % 5 + 1;
		int to = from + 5;
		return (from + "-" + to);

	}
}
