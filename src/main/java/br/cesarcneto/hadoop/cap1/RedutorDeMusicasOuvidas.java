package br.cesarcneto.hadoop.cap1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedutorDeMusicasOuvidas extends Reducer<Text, LongWritable, Text, LongWritable> {

	private final LongWritable totalDeVezes = new LongWritable(0);

	@Override
	protected void reduce(final Text musicaOuvida, final Iterable<LongWritable> quantidadeDeVezes, final Reducer<Text, LongWritable, Text, LongWritable>.Context contexto) throws IOException, InterruptedException {

		long acumulador = 0;
		for (final LongWritable quantidade : quantidadeDeVezes) {
			acumulador += quantidade.get();
		}

		totalDeVezes.set(acumulador);
		contexto.write(musicaOuvida, totalDeVezes);
	}

}
