package my;
        
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Voter 
{
	
	
	
	// Mapper for Mapping out the key-Value of 'List1'
        
	public static class List1 extends Mapper<Text, Text, Text, Text>
	{

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
        {
        	String[] words = value.toString().split("[ \t]+");
        	
        	String Voter = words[0];
        	
        	String Votee = words[1];
        	
        	context.write(new Text(Voter), new Text(Votee) );
        	
	        
	    }
	 }
	// End of 	Mapper for Mapping out the key-Value of 'List1'
	
	
	//---------------------------------------------------------------------------------//	
	
	
	// Mapper for Mapping out the key-Value of 'List2'
    
	public static class List2 extends Mapper<Text, Text, Text, Text>
	{

	    public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
	    {
        	String[] words = value.toString().split("[ \t]+");
        	
        	String Person = words[0];
        	
        	String Worth = words[1];
        	
        	context.write(new Text(Person), new Text(Worth) );
        	
	        
	    }
	}
	// End of 	Mapper for Mapping out the key-Value of 'List2'
			
	//---------------------------------------------------------------------------------//

	//  Beginning The Reducer1
	
	public static class ReducerOne extends Reducer<Text, Text, Text,LongWritable > 
	{

	 	public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException 
	    {
			// For each record check the file source; Store User Information and Store tweets info in a string 
			// to be split later using "\t" seperator
			
			String DiscardKey = key.toString();
			String[] Votees={""};
			int Voter_Worth = 0;
			int Num_of_Votees = 0;		
			
	    	while( values.iterator().hasNext())
	    	{
	    		String Str_Or_Num = values.iterator().next().toString();
	    		
	    		if (Str_Or_Num.matches(".*\\d.*"))
				{
	    			Voter_Worth = Integer.parseInt(Str_Or_Num);
	    			
				}
			else
				{
					Votees[Num_of_Votees] = Str_Or_Num;
					++Num_of_Votees;
				}
	    		
	    	}
	    	
	    	for(String eachVotee:Votees)
	    	{
	    		context.write(new Text(eachVotee), new LongWritable(Voter_Worth));
	    	}
	    }
}

	//	End Of Reducer1
	
	
	 	//---------------------------------------------------------------------------------//	
		
		
		// Dummy Mapper
	    
		public static class dummy_Mapper extends Mapper<Text, LongWritable, Text, LongWritable>
		{

		    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException 
		    {        	
	        	context.write(new Text(key), value);    
		    }
		}
		
		
		// End of Dummy Mapper 
		
		
		//---------------------------------------------------------------------------------//

	 	
		//  Beginning The Reducer2
		
		public static class ReducerTwo extends Reducer<Text,LongWritable, Text,LongWritable > 
		{

		 	public void reduce(Text key, Iterable<LongWritable > values, Context context) 
		      throws IOException, InterruptedException 
		    {
		 		 long Total_Worth = 0;
				  for(LongWritable Each_Voter_Worth:values)
				  {
					  Total_Worth += Each_Voter_Worth.get();
				  }
				  context.write(key, new LongWritable(Total_Worth));
		 		
		 		
		    }
		    
		}
		  
	 	
	//---------------------------------------------------------------------------//
	
	
	// The Driver
	
	
	
	 public static void main(String[] args) throws Exception 
	 {
		 
		 	JobConf conf_One = new JobConf();
		 	
		 	/*job_One --> concatenate two input file through 2 Mapper and
		 	 *Reduce the sorted output of the maps into Votee Worth combination 
		 	 */
		 	
			Job job_One = new Job(conf_One);
			JobControl jbcntrl_1=new JobControl("Voter_One");
			
			job_One.setJarByClass(Voter.class);
			
			/* MapReduce_ONE 
			* Multiple Mappers for reading List1 and List2 <List1.class and List2.class>
			*   Key--> List1:Voter/List2:Person   Value--> List1:Votee/List2:Worth
			* and Reducer for Reducing the above <ReducerOne.class>
			*   Key--> For each Voter Value: worth,Votee details
			*/
			MultipleInputs.addInputPath(conf_One, new Path(args[0]),TextInputFormat.class , List1.class);
		    MultipleInputs.addInputPath(conf_One, new Path(args[1]),TextInputFormat.class , List2.class);
		    job_One.setReducerClass(ReducerOne.class);
			job_One.setMapOutputKeyClass(Text.class);
			job_One.setMapOutputValueClass(LongWritable.class);
			job_One.setOutputKeyClass(Text.class);
			job_One.setOutputValueClass(LongWritable.class);
			FileOutputFormat.setOutputPath(job_One, new Path("Temp"));

		 	/*job_Two --> dummy mapper to prepare output for reducer
		 	 * to count and print the total worth of each Votee*/
			Job job_Two = new Job(conf_One);
			JobControl jbcntrl_2=new JobControl("Voter_One");
			job_Two.setJarByClass(Voter.class);
			/*MapReduce_TWO
			 * DummyMapper-->Prepare output for Reducer
			 * Reducer_Two-->
			 * For each Votee sum the worth of each voter
			*/
			FileInputFormat.addInputPath(job_Two, new Path("Temp"));
			job_Two.setMapperClass(dummy_Mapper.class);
			job_Two.setReducerClass(ReducerTwo.class);
			job_Two.setMapOutputKeyClass(Text.class);
			job_Two.setMapOutputValueClass(LongWritable.class);
			job_Two.setOutputKeyClass(Text.class);
			job_Two.setOutputValueClass(LongWritable.class);			
			FileOutputFormat.setOutputPath(job_Two, new Path(args[1]));
			
			job_Two.waitForCompletion(job_One.waitForCompletion(true));
	 }
	
}
			

	
