package Dictionary;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class CacheMapper  extends Mapper<LongWritable, Text, Text, Text> {
	String fName, language;
        File file;
        public Map<String, String> trs = new HashMap<String, String>();
           
        public void setup(Context context) throws IOException, InterruptedException{
            fName = context.getLocalCacheFiles()[0].toString();
            String[] fPath = fName.split("/");
            language = fPath[fPath.length-1].split("\\.")[0];
            file = new File(fName);
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = br.readLine()) != null) {
                StringTokenizer iterator = new StringTokenizer(line.toString(), "\t");
                if(iterator.countTokens() == 2) {
                    String engWord = iterator.nextToken();
                    String tr = iterator.nextToken();
                    String speech = this.getSpeech(tr);
                    if(!speech.equals("")){
                        tr = tr.substring(0, tr.lastIndexOf('[')).trim();
                        tr = tr.replace(";", ",");
                        trs.put(engWord + ": " + speech, language + ":" + tr);
                    }
                }
            }
            br.close();
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          StringTokenizer iterator = new StringTokenizer(value.toString(), "\t");
          String speech = iterator.nextToken();
          String tr = iterator.nextToken();
          if(trs.containsKey(speech)) {
                context.write(new Text(speech), new Text(tr + "|" + trs.get(speech)));
	  } else {
      		context.write(new Text(speech), new Text(tr + "|" + language + ":N/A"));
          }
        }
                                                                                                                              
	 private String getSpeech(String tr) {
  String[] validspeech = {"[Noun]", "[Verb]", "[Adverb]", "[Adjective]", "[Pronoun]","[Preposition]", "[Conjunction]"};
                                                                                                                                                                          String speech = "";
                                                                                                                                                                                      for(String item: validspeech) {                                                                                                                                                                                            
	if(tr.endsWith(item))	{
	speech = item;
                                                                                                                                                                                                                                       		break;
	}
}
                                                                                                                                                                                                                                                                                      return speech;
}	      
}

