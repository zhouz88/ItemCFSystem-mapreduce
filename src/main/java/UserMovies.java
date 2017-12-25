import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class UserMovies {

    public static class UserMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString().trim();
            String[] line = input.split(":");

            if (line.length < 3)
                throw new RuntimeException();

            context.write(new IntWritable(Integer.parseInt(line[0])), new Text(line[1] + ":" + line[2]));
        }
    }

    public static class UserReducer extends Reducer<IntWritable, Text , IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            StringBuilder sb = new StringBuilder();
            while (itr.hasNext()) {
                sb.append(itr.next());
                sb.append(',');
            }
            context.write(key, new Text(sb.toString().substring(0, sb.length() - 1)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(UserMovies.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
