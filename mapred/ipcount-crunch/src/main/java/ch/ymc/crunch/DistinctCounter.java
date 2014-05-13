package ch.ymc.crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Crunch based implementation of distinct IP counting in weblog data.
 * 
 * @author cguegi
 * @see http://crunch.apache.org/
 *
 */
public class DistinctCounter extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: hadoop jar wc-crunch-1.0-SNAPSHOT-jar-with-dependencies.jar"
							+ " [generic options] input output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return 1;
		}
		
		String inputPath = args[1];
		String outputPath = args[2];

		Pipeline pipeline = new MRPipeline(DistinctCounter.class, getConf());
		PCollection<String> lines = pipeline.readTextFile(inputPath);
		PCollection<String> words = lines.parallelDo(new IpDiscoverer(), Writables.strings());
		PTable<String, Long> counts = words.count();
		pipeline.writeTextFile(counts, outputPath);
		
		PipelineResult result = pipeline.done();
		
		return result.succeeded() ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new DistinctCounter(), args);
	}

}