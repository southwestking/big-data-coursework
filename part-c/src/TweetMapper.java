import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.util.List;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
   	static boolean dataLoaded = true;	
	private final IntWritable one = new IntWritable(1);
	static BufferedReader br;
	private List <String[]> list = new ArrayList<>();
	private final String[] supportPhrases = {"go", "team"}; 
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(dataLoaded){
			//gets file 
  			URI fileUri = context.getCacheFiles()[0];
        	FileSystem fs = FileSystem.get(context.getConfiguration());
        	FSDataInputStream in = fs.open(new Path(fileUri));
			br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
 			String line = null;
			try {
				
				br.readLine();
        		while ((line = br.readLine()) != null)
					list.add(line.toLowerCase().split(","));
		 
 				br.close();
				 
			} catch (IOException e1) {
        
			}		
			dataLoaded=false;
		}

		 String tages = getHashTags(value).toLowerCase(); 		
		context.write(new Text(checkHashtags(tages)), one);
    }
		
	public String checkHashtags(String tag){
		for (String[] entries : list){
				for(String entry :entries)
					if (tag.contains(supportPhrases[0]+entry) || tag.contains(supportPhrases[1]+entry))
						return entries[0];
				
		}
		return "uncounted";	
	}

	public String getHashTags(Text value){
		return value.toString().split(";")[2];
	}
}