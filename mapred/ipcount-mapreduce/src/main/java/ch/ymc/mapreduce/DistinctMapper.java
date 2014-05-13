package ch.ymc.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author cguegi
 *
 */
public class DistinctMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final String REGEX = "^([\\d.]+) \\S+ \\S+ \\[(.+?)\\] \\\"(.+?)\\\" (\\d{3}) (\\d+) \\\"(.*?)\\\" \\\"(.+?)\\\" \\\"SESSIONID=(\\d+)\\\"\\s*";
	private Pattern pattern;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/**
		 * 10.34.198.119 - - [31/May/2013:00:00:00 -0800]
		 * "GET /about/careers HTTP/1.1" 200 48091
		 * "http://www.google.com/search?q=laptop" "Mozilla/5.0 (Windows NT 6.1;
		 * WOW64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43
		 * Safari/537.31" "SESSIONID=743021225616"
		 */
		String line = value.toString();
		Matcher matcher = this.pattern.matcher(line);
		if (matcher.find()) {
			String ip = matcher.group(1);
			if (StringUtils.isNotBlank(ip)) {
				context.write(new Text(ip), new IntWritable(1));
			}
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.pattern = Pattern.compile(REGEX);
	}
}
