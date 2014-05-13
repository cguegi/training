package ch.ymc.cascading;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * Cascading based implementation of distinct IP's counting in weblog data.
 * 
 * @author cguegi
 * @see http://www.cascading.org/
 *
 */
public class Driver {
	private static final String REGEX 
		= "^([\\d.]+) \\S+ \\S+ \\[(.+?)\\] \\\"(.+?)\\\" (\\d{3}) (\\d+) \\\"(.*?)\\\" \\\"(.+?)\\\" \\\"SESSIONID=(\\d+)\\\"\\s*";
	
	public static void main(String[] args) {

		Properties properties = new Properties();
		AppProps.setApplicationJarClass( properties, Driver.class );
		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		
		
		String inputPath = args[1];
		String outputPath = args[2];
//		System.out.println("input: " + inputPath);
//		System.out.println("output: " + outputPath);
		
		Tap<?,?,?> source = new Hfs(new TextLine(new Fields("offset", "line")), inputPath);
		Tap<?,?,?> sink = new Hfs(new TextLine(1), outputPath, SinkMode.REPLACE);
		
		Fields apacheFields = new Fields("ip", "date", "request", "status", "byte", "referer", "ua", "cookie");
	    int[] apacheGroups = {1, 2, 3, 4, 5, 6, 7, 8};
	    RegexParser parser = new RegexParser(apacheFields, REGEX, apacheGroups);
		
		Pipe wcPipe = new Each("ipcount", new Fields("line"), parser, new Fields("ip"));
		
		// group on "ips"
		wcPipe = new GroupBy(wcPipe, new Fields("ip"));
		wcPipe = new Every(wcPipe, new Count(), new Fields( "ip", "count" ));
		
	    Flow<?> count = flowConnector.connect(source, sink, wcPipe);
	    count.complete();
		
	}

}