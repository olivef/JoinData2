package com.oliveirf.hadooptraining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import java.util.ArrayList;
import java.util.List;

public class JoinData2 {

    static List<Text> wikilist = new ArrayList<Text>();
    static List<Text> dbpedialist = new ArrayList<Text>();

    public static class DbpediaMapper extends Mapper<LongWritable, Text, Text,Text> {
        private Text foreign_key = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split(" ");
            foreign_key.set(valor[0]);
	    System.err.println("foreign PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP="+foreign_key.toString()+"fsdfsdfsd"+valor[0]);
	    Text result=new Text("D"+value);
	    context.write(foreign_key, result);
        }
    }

    public static class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text foreign_key = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valor = value.toString().split(" ");
            foreign_key.set(valor[1]);
           System.err.println("foreign dentro do wiki="+foreign_key.toString()+"sasasasa"+valor[0]);
	    Text result=new Text("W"+value);
            context.write(foreign_key, result);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            wikilist.clear();
            dbpedialist.clear();
	    //Iterator it = myEntries.iterator();
        
	    for (Text val : values) {
        	 System.err.println("dentro do reducer="+val);
	        if (val.charAt(0) == 'D') {
                    dbpedialist.add(new Text(val.toString().substring(1)));
                } else {
                    wikilist.add(new Text(val.toString().substring(1)));
                }
            }
            if (!wikilist.isEmpty() && !dbpedialist.isEmpty()) {
                for (Text W : wikilist) {
                    for (Text D : dbpedialist) {
                        context.write(W, D);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path wikistats = new Path(otherArgs[0]);
        Path dbpedia = new Path(otherArgs[1]);

        Path join_result = new Path(otherArgs[2]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(JoinData2.class);
        job.setJobName("JoinData2");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, wikistats, TextInputFormat.class, WikiMapper.class);
        MultipleInputs.addInputPath(job, dbpedia, TextInputFormat.class, DbpediaMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        TextOutputFormat.setOutputPath(job, join_result);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

