import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class NormalizeMatrix {
    public static class NormalizeMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //movie:movie\t45
            String[] line = value.toString().trim().split("\t");
            String[] movieTomovie = line[0].split(":");
//            System.out.println(Integer.parseInt(line[1]));
//            System.out.println(Integer.parseInt(movieTomovie[0]));
//            System.out.println(Integer.parseInt(movieTomovie[1]));
            String res = movieTomovie[1].trim() + "=" + line[1].trim();
            context.write(new Text(movieTomovie[0]), new Text(res));
        }
    }

    public static class NormalizeMatrixReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int denominator = 0;
            Iterator<Text> itr = values.iterator();
            List<String> list = new ArrayList<String>();
            List<Integer> res = new ArrayList<Integer>();

            while (itr.hasNext()) {
                String l1 = itr.next().toString();
                int index = l1.indexOf("=");
                list.add(l1.substring(0, index));
                int total = Integer.parseInt(l1.substring(index+1).trim());
                res.add(total);
                denominator += Integer.parseInt(l1.substring(index+1).trim());
            }

            for (int i = 0; i < list.size(); i++)
                context.write(new Text(key.toString()+"->"+list.get(i)), new Text(res.get(i)*1.0/denominator + ""));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(NormalizeMatrix.class);
        job.setMapperClass(NormalizeMatrixMapper.class);
        job.setReducerClass(NormalizeMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
