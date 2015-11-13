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
	
	public Text getDate(Text value){
		String[] tweetData = value.toString().split(";");
		String[] dates = tweetData[1].split(",");
		return new Text(dates[0]);
	}
}
