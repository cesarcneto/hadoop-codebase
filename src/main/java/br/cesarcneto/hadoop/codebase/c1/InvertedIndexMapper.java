package br.cesarcneto.hadoop.codebase.c1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public static final int RETAIlER_INDEX = 0;

	public void map(final LongWritable longWritable, final Text text, final OutputCollector<Text, Text> outputCollector, final Reporter reporter) throws IOException {
		final String[] record = StringUtils.split(text.toString(), ",");
		final String retailer = record[RETAIlER_INDEX];
		for (int i = 1; i < record.length; i++) {
			final String keyword = record[i];
			outputCollector.collect(new Text(keyword), new Text(retailer));
		}
	}
}
