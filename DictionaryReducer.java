package Dictionary;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DictionaryReducer extends Reducer<Text,Text,Text,Text> {
   public void reduce(Text word, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
	String str[]=new String[5];
	String temp="";
	String w = word.toString();
	for(int i=0;i<5;i++)
		str[i]="";

	for(Text t : values){
		String t1 = t.toString().substring(0,t.toString().indexOf(":"));				if(t1.contains("french")){
			str[0]+=t.toString().substring(t.toString().indexOf(":"))+",";
		}else if(t1.contains("german")){
			str[1]+=t.toString().substring(t.toString().indexOf(":"))+",";
		}else if(t1.contains("italian")){
			str[2]+=t.toString().substring(t.toString().indexOf(":"))+",";
		}else if(t1.contains("portugese")){
			str[3]+=t.toString().substring(t.toString().indexOf(":"))+",";
		}else if(t1.contains("spanish")){
			str[4]+=t.toString().substring(t.toString().indexOf(":"))+",";
		}		    
	}
	if(str[0].isEmpty())
		temp = "french:N/A|";
	else
		temp = "french:"+str[0].substring(0,str[0].length()-1)+"|";

	if(str[1].isEmpty())
		temp+="german:N/A|";
	else
		temp+="german:"+str[1].substring(0,str[1].length()-1)+"|";

	if(str[2].isEmpty())
		temp+="italian:N/A|";
	else
		temp+="italian:"+str[2].substring(0,str[2].length()-1)+"|";

	if(str[3].isEmpty())
		temp+="portugese:N/A|";
	else
		temp+="portugese:"+str[3].substring(0,str[3].length()-1)+"|";

	if(str[4].isEmpty())
		temp+="spanish:N/A";
	else
		temp+="spanish:"+str[4].substring(0,str[4].length()-1);
	
	context.write(word,new Text(temp));
	}
}
