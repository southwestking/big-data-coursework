//import java.io.IOException;
import java.util.StringTokenizer;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    
	private final IntWritable one = new IntWritable(1);
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
  URI fileUri = context.getCacheFiles()[0];
	Map<String, String> map = new HashMap<String, String>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));
 	String line = null;
	try {
		 br.readLine();

        	while ((line = br.readLine()) != null) {
			String[] fields = line.split(",");
			map.put(fields[0],fields[1]);
		}
 br.close();
	} catch (IOException e1) {
        
	}		




		String[] tages = getHashTags(value);
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

/**
		0 = tweet ID
		1 = data/time
		2 = hashtages
		3 = tweet
	*/
	public String[] getHashTags(Text value){

		String[] tweetData = value.toString().split(";");
		return tweetData[2].split(" ");
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
