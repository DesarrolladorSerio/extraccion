import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempPerCity {

    static class Record {
        String city;
        int tmin;
        int tmax;
        boolean ok;

        static final Pattern REG = Pattern.compile("^\\s*(.+?)\\s+([0-9]{4}[-/][0-9]{2}[-/][0-9]{2})\\s+(\\d+)\\s+(\\d+)\\s*$");

        static Record parse(String line) {
            Record r = new Record();
            if (line == null) { r.ok = false; return r; }
            String s = line.trim();
            if (s.isEmpty()) { r.ok = false; return r; }

            String[] trySemi = s.split(";");
            if (trySemi.length == 4) {
                r.city = trySemi[0].trim();
                r.tmin = parseIntSafe(trySemi[2]);
                r.tmax = parseIntSafe(trySemi[3]);
                r.ok = r.city.length() > 0;
                return r;
            }
            String[] tryComma = s.split(",");
            if (tryComma.length == 4) {
                r.city = tryComma[0].trim();
                r.tmin = parseIntSafe(tryComma[2]);
                r.tmax = parseIntSafe(tryComma[3]);
                r.ok = r.city.length() > 0;
                return r;
            }
            Matcher m = REG.matcher(s);
            if (m.matches()) {
                r.city = m.group(1).trim();
                r.tmin = parseIntSafe(m.group(3));
                r.tmax = parseIntSafe(m.group(4));
                r.ok = r.city.length() > 0;
                return r;
            }
            r.ok = false;
            return r;
        }

        static int parseIntSafe(String x) {
            try { return Integer.parseInt(x.trim()); } catch (Exception e) { return Integer.MIN_VALUE; }
        }
    }

    public static class MapMax extends Mapper<Object, Text, Text, IntWritable> {
        private final Text cityOut = new Text();
        private final IntWritable tempOut = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            Record r = Record.parse(value.toString());
            if (!r.ok || r.tmax == Integer.MIN_VALUE) return;
            cityOut.set(r.city);
            tempOut.set(r.tmax);
            ctx.write(cityOut, tempOut);
        }
    }

    public static class ReduceMax extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable out = new IntWritable();
        @Override
        protected void reduce(Text city, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (IntWritable v : values) {
                if (v.get() > max) max = v.get();
            }
            if (max != Integer.MIN_VALUE) {
                out.set(max);
                ctx.write(city, out);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: MaxTempPerCity <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxTempPerCity");
        job.setJarByClass(MaxTempPerCity.class);

        job.setMapperClass(MapMax.class);
        job.setReducerClass(ReduceMax.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
