package ch.ymc.crunch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * Discovers IP's from Apache logs.
 * 
 * @author cguegi
 *
 */
public class IpDiscoverer extends DoFn<String, String> {

	private static final long serialVersionUID = -591487212390585664L;
	private static final String REGEX = 
			"^([\\d.]+) \\S+ \\S+ \\[(.+?)\\] \\\"(.+?)\\\" (\\d{3}) (\\d+) \\\"(.*?)\\\" \\\"(.+?)\\\" \\\"SESSIONID=(\\d+)\\\"\\s*";
	private Pattern pattern = Pattern.compile(REGEX);

	@Override
	public void process(String line, Emitter<String> emitter) {

		Matcher matcher = this.pattern.matcher(line);
		if (matcher.find()) {
			String ip = matcher.group(1);
			if (StringUtils.isNotBlank(ip)) {
				emitter.emit(ip);
			}
		}
	}

}