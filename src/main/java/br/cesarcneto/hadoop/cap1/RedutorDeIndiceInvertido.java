package br.cesarcneto.hadoop.cap1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RedutorDeIndiceInvertido extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(final Text texto, final Iterator<Text> textoIterator, final OutputCollector<Text, Text> coletorDeSaida, final Reporter relator) throws IOException {
		final String lojas = StringUtils.join(textoIterator, ',');
		coletorDeSaida.collect(texto, new Text(lojas));
	}
}
