package pivotHadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

public class MapReduce {
	 public static class PivotMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 

		 		long col = 0;
		 		long line = key.get();
		 		//Découpage des lignes avec le délimiteur par défaut des CSV, le point-virgule
		 		for (String num : value.toString().split(";")) {
		 			//Créer une colonne pour chaque valeur récupérée
		 			context.write(new LongWritable(col), new Text(line + "\t" + num));
		 			++col;
		 		}		 			
		 	}
	 }
	
	 public static class PivotReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		 @Override
		 protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 TreeMap<Long, String> line = new TreeMap<Long, String>();
             for (Text text : values) {
            	 //Récupération dans une liste les valeurs délimités par un tab
                 String[] parts = text.toString().split("\t");
                 line.put(Long.valueOf(parts[0]), parts[1]);
             }
             String rowString = StringUtils.join(line.values(), ' ');
             //Ecrit dans le context, la clé de ligne et la ligne
             context.write(key, new Text(rowString));
      }
		 }
	
	 public static void main(String[] args) throws Exception {
		    //Configurations de l'environnement Hadoop
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Pivot Hadoop");
		    job.setJarByClass(MapReduce.class);
		    job.setMapperClass(PivotMapper.class);
		    job.setReducerClass(PivotReducer.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
		 }

