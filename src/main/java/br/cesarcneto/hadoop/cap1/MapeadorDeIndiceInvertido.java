package br.cesarcneto.hadoop.cap1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapeadorDeIndiceInvertido extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public static final int INDICE_LOJA = 0;

	public void map(final LongWritable longWritable, final Text texto, final OutputCollector<Text, Text> coletorDeSaida, final Reporter relator) throws IOException {
		final String[] registro = StringUtils.split(texto.toString(), ",");
		final String loja = registro[INDICE_LOJA];
		for (int i = 1; i < registro.length; i++) {
			final String palavraChave = registro[i];
			coletorDeSaida.collect(new Text(palavraChave), new Text(loja));
		}
	}
}
