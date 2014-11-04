package br.cesarcneto.hadoop.cap1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TarefaProcessaMusicasPopularesTeste {

	private MapeadorDeMusicasOuvidas mapeador;
	private MapDriver<NullWritable, Text, Text, LongWritable> driverDeMapeamento;

	private <CHAVE, VALOR> List<Pair<CHAVE, VALOR>> criaListaDePares(final Pair<CHAVE, VALOR>... pares) {

		final List<Pair<CHAVE, VALOR>> lista = new ArrayList<>();
		for (final Pair<CHAVE, VALOR> par : pares) {
			lista.add(par);
		}

		return lista;
	}

	private List<Pair<NullWritable, Text>> getListaDeValoresEntrada() {
		return criaListaDePares(//
				new Pair<NullWritable, Text>(NullWritable.get(), new Text("20141030 20h15m55s: genero/pop/artista/clean-bandit-ft-jess-glynne/musica/rather-be/ouvir")), //
				new Pair<NullWritable, Text>(NullWritable.get(), new Text("20141030 20h16m01s: genero/rock/artista/malta/musica/diz-pra-mim/ouvir")), //
				new Pair<NullWritable, Text>(NullWritable.get(), new Text("20141030 20h16m15s: genero/sertanejo/artista/marcos-e-belutti/musica/domingo-de-manha/ouvir")));
	}

	@Before
	public void setUp() throws Exception {
		mapeador = new MapeadorDeMusicasOuvidas();
		driverDeMapeamento = MapDriver.newMapDriver(mapeador);
	}

	@Test
	public void testaMapeamentoComApenasUmRegistro() throws Exception {

		final NullWritable chaveDeEntrada = NullWritable.get();
		final Text valorDeEntrada = new Text("20141030 20h15m55s: genero/pop/artista/clean-bandit-ft-jess-glynne/musica/rather-be/ouvir");

		final Pair<Text, LongWritable> saidaEsperada = new Pair<Text, LongWritable>(new Text("rather-be"), new LongWritable(1l));

		driverDeMapeamento.withInput(chaveDeEntrada, valorDeEntrada)//
		.withOutput(saidaEsperada) //
		.runTest();
	}

	@Test
	public void testaMapeamentoComNRegistros() throws Exception {

		final List<Pair<NullWritable, Text>> listaDeEntrada = getListaDeValoresEntrada();

		final List<Pair<Text, LongWritable>> listDeSaidaEsperada = criaListaDePares(//
				new Pair<Text, LongWritable>(new Text("rather-be"), new LongWritable(1l)), //
				new Pair<Text, LongWritable>(new Text("diz-pra-mim"), new LongWritable(1l)), //
				new Pair<Text, LongWritable>(new Text("domingo-de-manha"), new LongWritable(1l)));

		driverDeMapeamento.withAll(listaDeEntrada)//
				.withAllOutput(listDeSaidaEsperada) //
				.runTest();
	}
}
