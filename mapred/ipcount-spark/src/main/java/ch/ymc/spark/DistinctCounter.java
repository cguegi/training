package ch.ymc.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/**
 * Spark implementation of counting distinct IP's in weblog data.
 * 
 * @author cguegi
 *
 */
public class DistinctCounter {
	private static final Pattern REGEX = 
			Pattern.compile("^([\\d.]+) \\S+ \\S+ \\[(.+?)\\] \\\"(.+?)\\\" (\\d{3}) (\\d+) \\\"(.*?)\\\" \\\"(.+?)\\\" \\\"SESSIONID=(\\d+)\\\"\\s*");
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: DistinctCounter <master> <in> <out>");
			System.exit(1);
		}
		
		String input = args[1];
		String output = args[2];

		JavaSparkContext ctx = new JavaSparkContext(
				args[0], 
				"DistinctCounter",
				System.getenv("SPARK_HOME"),
				JavaSparkContext.jarOfClass(DistinctCounter.class));
		
		JavaRDD<String> lines = ctx.textFile(input, 1);

		JavaRDD<String> ips = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				Matcher matcher = REGEX.matcher(s);
				List<String> result = new ArrayList<String>(0);
				if (matcher.find()) {
					String ip = matcher.group(1);
					if (StringUtils.isNotBlank(ip)) {
						result.add(ip);
					}
				}
				return result;
			}
		});

		JavaPairRDD<String, Integer> ones = ips.map(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		counts.saveAsTextFile(output);
		System.exit(0);
	}

}