package Dictionary;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DictionaryMapper  extends Mapper<Text, Text, Text, Text> {
      // TODO define class variables for translation, language, and file name
	String filename;
      public void setup(Context context) { 
		filename = ((FileSplit) context.getInputSplit()).getPath().getName();   	   
		String[] fname = filename.split("\\.");
		filename = fname[0];
	}

      public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      // TODO instantiate a tokenizer based on a regex for the file
	String word = key.toString();
	String data = value.toString();
	if(data.endsWith("[Noun]") || data.endsWith("[Adverb]") || data.endsWith("[Conjunction]") || data.endsWith("[Adjective]") || data.endsWith("[Preposition]") || data.endsWith("[Pronoun]") || data.endsWith("[Verb]")){
		word = word+": "+data.substring(data.indexOf("["));
		data = filename+":"+data.substring(0,data.indexOf("["));	
		context.write(new Text(word),new Text(data));
	}
      // iterate through the tokens of the line, parse, transform and write to context
      }
}
