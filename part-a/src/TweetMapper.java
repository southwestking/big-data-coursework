import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // while (itr.hasMoreTokens()) {
        //   data.set(itr.nextToken().toLowerCase());
        //   context.write(data, one);
		//}
		//0 = tweet ID
		//1 = data/time
		//2 = hashtages
		//3 = tweet  
		String[] tweetData = value.toString().split(";");
		StringTokenizer itr = new StringTokenizer(tweetData[3], " \t\n\r\f,.:;?!#[]'");
		
		
		
		
		while (itr.hasMoreTokens()) {
			//int rounds down for 1-5...
			int tweetSize = itr.nextToken().length();
			if(tweetSize % 5 == 0)
				//this keeps it in the 1-5,6-10 range
				tweetSize = tweetSize-1; 
			int limitRange = tweetSize/5;
			data.set(limiterString(limitRange));
			context.write(data, one);
        }
    }
	
	public String limiterString(int value){
		
		if (value==0)
			return (1 + "-" + 5);
		else{
			int from = (value * 5)+1;
			int to = from + 5;
			return (from + "-" + to);
		}
	}
}