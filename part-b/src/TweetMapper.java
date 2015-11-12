import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    
	private final IntWritable one = new IntWritable(1);
	
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
		
		context.write(getDate(value), one);       
    }
	/**
		0 = tweet ID
		1 = data/time
		2 = hashtages
		3 = tweet
	*/
	public Text getDate(Text value){
		String[] tweetData = value.toString().split(";");
		String[] dates = tweetData[1].split(",");
		return new Text(dates[0]);
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
