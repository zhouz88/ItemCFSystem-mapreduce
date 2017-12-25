import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class MatrixMultipicaliton {
    public static class MatrixMultipicalitonMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            String s = value.toString().trim();
            int start = s.indexOf(">");
            int end = s.indexOf("\t");
            String k = s.substring(start + 1, end);
            String t = s.substring(0, start - 1) + "=" + s.substring(end+1);
            context.write(new Text(k), new Text(t));
        }
        //MOVIE->MOVIE\T0.333
        //output moive     movie1=0.545;
    }

    public static class MatrixMultipicalitonUserMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            String s = value.toString().trim();
            int start = s.indexOf(",");
            int end = s.lastIndexOf(",");
            String k = s.substring(start + 1, end);
            String t = s.substring(0, start) + "-" + s.substring(end+1);
            context.write(new Text(k), new Text(t));
        }
        //user:movie:rating
        //output movie\tuser-3.4
    }

    public static class MatrixMultipicalitonReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movies = new ArrayList<String>();
            List<String> users = new ArrayList<String>();
            Iterator<Text> itr = values.iterator();

            while (itr.hasNext()) {
                String tmp = itr.next().toString();
                if (tmp.contains("=")) {
                    movies.add(tmp);
                } else {
                    users.add(tmp);
                }
            }

            for (String a : movies) {
                for (String b : users) {
                    String[] movie = a.split("=");
                    String[] user = b.split("-");
                    double res = Double.parseDouble(movie[1]) * Double.parseDouble(user[1]);
                    context.write(new Text(movie[0] + ":" + user[0]), new Text(res+""));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixMultipicaliton.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, MatrixMultipicalitonMovieMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, MatrixMultipicalitonUserMapper.class, Text.class, Text.class, Text.class, Text.class, conf);


//        job.setMapperClass(MatrixMultipicalitonMovieMapper.class);
//        job.setMapperClass(MatrixMultipicalitonUserMapper.class);
        job.setReducerClass(MatrixMultipicalitonReducer.class);

//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixMultipicalitonMovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixMultipicalitonUserMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

    /*

    	Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
     */
}
