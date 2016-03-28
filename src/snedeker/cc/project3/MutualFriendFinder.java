package snedeker.cc.project3;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MutualFriendFinder {
	
	/**
	 * This is the Mapper component.  It will take the list of users and friends and use them
	 * to generate user,user friends pairs for every user a user is friends with.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		// Create the variables that will hold the (i,j) key, and (matrix, j, number) value
		private Text friendPair = new Text();
		private Text allFriends = new Text();
		
		/**
		 * This is the map function.  In this function the lines are read and tokenized.  The 
		 * user and friend information is converted to a series of pairs.  The keys for these 
		 * pairs consist of the user and each friend, and the value is the complete list of
		 * friends.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Read in the first line
			String line = value.toString();
			
			// Split the line on the "," delimiter
			// The resultant String array values are
			// value[0] - user, value[1...n] - friends separated by ' '
			String[] personAndFriends = line.split(",");
			
			String person = personAndFriends[0];
			String[] friends = personAndFriends[1].split(" ");
			
			for (String friend : friends) {
				friendPair.set(person.compareTo(friend) < 0 ? person + "," + friend : friend + "," + person);
				allFriends.set(personAndFriends[1]);
				context.write(friendPair, allFriends);
			}
		}
	}
	
	/**
	 * This is the Reducer component.  It will take the Mapped, Shuffled, and Sorted data,
	 * and generate the lists of mutual friends for each user.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/**
		 * This is the reduce function.  It takes all of the entries for this (i,k) 
		 * position of the result matrix, sorts them by j value, multiplies the 
		 * matching pairs, and sums the results together to find the value to place
		 * into the resultant matrix.
		 */
		public void reduce(Text key, Iterable<Text> allFriends, Context context) throws IOException, InterruptedException {
			
			String mutualFriends = "";
			TreeMap<String, Integer> friendOrdererAndCounter = new TreeMap<>();
			
			// Take each friend list for this pair of users and sort their friends into a TreeMap.
			// If a friend is not yet in the v then insert them with a value of one.  If the
			// friend is in the TreeMap already then add one to their value.  Any friends with only
			// a value of one at the end of sorting is not present in both lists.
			for (Text text : allFriends) {
				for (String friend : text.toString().split(" ")) {
					
					Integer friendInstanceCount = friendOrdererAndCounter.get(friend);
					
					if (friendInstanceCount != null) {
						friendOrdererAndCounter.put(friend, 2);
					}
					else {
						friendOrdererAndCounter.put(friend, 1);
					}
				}
			}
			
			boolean firstFriend = true;
			for (Entry<String, Integer> entry: friendOrdererAndCounter.entrySet()) {
				
				if (entry.getValue() > 1)
				{
					if (firstFriend) {
						mutualFriends = mutualFriends + entry.getKey();
						firstFriend = false;
					}
					else
						mutualFriends = mutualFriends + "," + entry.getKey();
				}
			}
			
			context.write(key, new Text("[" + mutualFriends + "]"));
		}
	}
	
	/**
	 * Configures the Hadoop job, and reads the user provided arguments
	 * 
	 * @param args The user provided arguments.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//Get configuration object and set a job name
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", " ");
		Job job = new Job(conf, "mutualFriendFinder");
		job.setJarByClass(snedeker.cc.project3.MutualFriendFinder.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//Set key, output classes for the job (same as output classes for Reducer)
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Set format of input files; "TextInputFormat" views files
		//as a sequence of lines
		job.setInputFormatClass(TextInputFormat.class);
		//Set format of output files: lines of text
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setNumReduceTasks(2); #set num of reducers
		//accept the hdfs input and output directory at run time
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Launch the job and wait for it to finish
//		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
