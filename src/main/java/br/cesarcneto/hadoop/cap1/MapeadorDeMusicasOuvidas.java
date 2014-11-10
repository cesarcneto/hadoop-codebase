package br.cesarcneto.hadoop.cap1;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapeadorDeMusicasOuvidas extends Mapper<NullWritable, Text, Text, LongWritable> {

	private static final LongWritable UMA_VEZ = new LongWritable(1);
	private final Text musicaOuvida = new Text();

	private final Pattern padrao = Pattern.compile("\\/musica\\/(\\w|\\-)+\\/");

	private String extraiMusicaOuvidaDoLog(final Text registroDeLog) {

		final Matcher matcher = padrao.matcher(registroDeLog.toString());
		if (matcher.find()) {
			return matcher.group().replaceAll("\\/musica\\/", "").replaceAll("\\/", "");
		}

		return null;
	}

	@Override
	protected void map(final NullWritable chaveNula, final Text registroDeLog, final Context contexto) throws IOException, InterruptedException {
		final String musicaOuvidaDoLog = extraiMusicaOuvidaDoLog(registroDeLog);
		if (musicaOuvidaDoLog != null) {
			musicaOuvida.set(musicaOuvidaDoLog);
			contexto.write(musicaOuvida, UMA_VEZ);
		}
	}

}
