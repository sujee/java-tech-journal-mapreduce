package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CampaignMR1 extends Configured implements Tool
{

    static class MyMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {

        @Override
        public void map(Object key, Text record, Context context) throws IOException
        {
            // timestamp, user_id, action_id, domain, campaign_id
            String[] tokens = record.toString().split(",");
            String actionStr = tokens[2];
            String campaignStr = tokens[4];

            try
            {
                int action = Integer.parseInt(actionStr);
                int campaign = Integer.parseInt(campaignStr);

                IntWritable mapOutKey = new IntWritable(campaign);
                IntWritable mapOutValue = new IntWritable(action);
                context.write(mapOutKey, mapOutValue);
            } catch (Exception e)
            {
                System.out.println("*** exception:");
                e.printStackTrace();
            }

        }

    }

    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
    {

        public void reduce(IntWritable key, Iterable<IntWritable> results, Context context) throws IOException,
                InterruptedException
        {

            int campaign = key.get();
            int views = 0;
            int clicks = 0;
            for (IntWritable i : results)
            {
                int action = i.get();
                if (action == 1)
                    views++;
                else if (action == 2)
                    clicks++;
            }
            String stats = "views=" + views + ",  clicks=" + clicks;
            context.write(new IntWritable(campaign), new Text(stats));
        }

    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new CampaignMR1(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.out.println("usage : need <input path>  <output path>");
            return 1;
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();

        Job job = new Job(conf, "CampaignMR1");
        job.setJarByClass(CampaignMR1.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);
        // job.setNumReduceTasks(7);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
