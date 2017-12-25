import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Sum {

    public static class UserMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString().trim();
            String[] array = input.split("\t");
            context.write(new Text(array[0]), new DoubleWritable(Double.parseDouble(array[1])));
        }
    }

    public static class UserReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0.0;
            for (DoubleWritable value : values) {
                total += value.get();
            }
            context.write(key, new DoubleWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        //configuration.set("dictionary", args[2]);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(UserMovies.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
