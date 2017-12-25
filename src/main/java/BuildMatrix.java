import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class BuildMatrix {
    public static class BuildMatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");
            //System.out.println(value.toString());
            if (line.length != 2)
                throw new RuntimeException();

            String[] moviesUsers = line[1].split(",");
            if (moviesUsers.length == 0)
                throw new RuntimeException();

            String[] movies = new String[moviesUsers.length];

            for (int i = 0; i < moviesUsers.length; i++) {
                int k = moviesUsers[i].indexOf(":");
                movies[i] = moviesUsers[i].substring(0, k);
            }

            int len = movies.length;

            for (int i = 0; i < len; i++)
                for (int j = 0; j < len; j++) {
                    String tmp = movies[i] + ":" + movies[j];
                    context.write(new Text(tmp), new IntWritable(1));
                }
        }
    }

    public static class BuildMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> itr = values.iterator();
            StringBuilder sb = new StringBuilder();
            int total = 0;
            while (itr.hasNext())
                total += itr.next().get();

            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //configuration.set("dictionary", args[2]);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(UserMovies.class);
        job.setMapperClass(BuildMatrixMapper.class);
        job.setReducerClass(BuildMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
