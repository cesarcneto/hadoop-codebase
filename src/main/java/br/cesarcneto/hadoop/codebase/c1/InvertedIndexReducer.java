package br.cesarcneto.hadoop.codebase.c1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(final Text text, final Iterator<Text> textIterator, final OutputCollector<Text, Text> outputCollector, final Reporter reporter) throws IOException {
		final String retailers = StringUtils.join(textIterator, ',');
		outputCollector.collect(text, new Text(retailers));
	}
}
