import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.ArrayList;
import java.util.List;

public class NormalizeMatrix {
    public static class NormalizeMatrixMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //movie:movie\t45
            String[] line = value.toString().trim().split("\t");
            String[] movieTomovie = line[0].split(":");
            context.write(new Text(movieTomovie[0]), new Text(movieTomovie[1] + "=" + line[1]));
        }
    }

    public static class NormalizeMatrixReducer extends Reducer<Text, Text, Text, DoubleWritable> {

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
                int total = Integer.parseInt(l1.substring(index+1));
                res.add(total);
                denominator += Integer.parseInt(l1.substring(index+1));
            }

            for (int i = 0; i < list.size(); i++)
                context.write(new Text(key.toString()+"->"+list.get(i)), new DoubleWritable(res.get(i)*1.0/denominator));

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
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
