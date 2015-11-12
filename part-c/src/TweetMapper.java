import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    
	private final IntWritable one = new IntWritable(1);
	TweetData tweetObj = new TweetData();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
		tweetObj.setFromString(value.toString());
		String[] tages = tweetObj.getSplitTages();
		for(String tag : tages)
			context.write(new Text(checkHashtages(tag)), one);
        
    }
	
		
	public String checkHashtages(String tag){
		
		if(tag.contains("gogb") || tag.contains("teamgb"))
			return "Great Britain";
		if(tag.contains("gousa") || tag.contains("teamusa"))
			return "United States of America";
		if(tag.contains("gojam") || tag.contains("teamjam"))
			return "Jamaica";
		if(tag.contains("gojor") || tag.contains("teamjor"))
			return "Jordan";
		if(tag.contains("goita") || tag.contains("teamita"))
			return "Italy";		
		if(tag.contains("gokaz") || tag.contains("teamkaz"))
			return "Kazakhstan";
		if(tag.contains("gorus") || tag.contains("teamrus"))
			return "Russia";
		if(tag.contains("gokor") || tag.contains("teamkor"))
			return "South Korea";
		if(tag.contains("goarg") || tag.contains("teamarg"))
			return "Argentina";	
			
			
		return "not counted";	
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
