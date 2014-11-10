package br.cesarcneto.hadoop.cap1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TarefaProcessaMusicasPopularesTeste {

	private static final boolean ORDEM_NAO_IMPORTA = false;

	private static final LongWritable UM = new LongWritable(1l);
	private static final LongWritable DOIS = new LongWritable(2l);
	private static final LongWritable TRES = new LongWritable(3l);
	private static final LongWritable QUATRO = new LongWritable(4l);
	private static final LongWritable CINCO = new LongWritable(5l);

	private MapDriver<NullWritable, Text, Text, LongWritable> driverDeMapeamento;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> driverDeReducao;
	private MapReduceDriver<NullWritable, Text, Text, LongWritable, Text, LongWritable> driverDeMapeamentoEReducao;

	private <CHAVE, VALOR> List<Pair<CHAVE, VALOR>> criaListaDePares(final Pair<CHAVE, VALOR>... pares) {

		final List<Pair<CHAVE, VALOR>> lista = new ArrayList<>();
		for (final Pair<CHAVE, VALOR> par : pares) {
			lista.add(par);
		}

		return lista;
	}

	private List<Pair<Text, LongWritable>> getListaDeMusicasMapeadas() {
		return criaListaDePares(//
				new Pair<Text, LongWritable>(new Text("rather-be"), UM), //
				new Pair<Text, LongWritable>(new Text("diz-pra-mim"), UM), //
				new Pair<Text, LongWritable>(new Text("domingo-de-manha"), UM));
	}

	private List<Pair<NullWritable, Text>> getListaDeValoresEntrada() {
		return criaListaDePares(//
				new Pair<NullWritable, Text>(NullWritable.get(), new Text("20141030 20h15m55s: genero/pop/artista/clean-bandit-ft-jess-glynne/musica/rather-be/ouvir")), //
				new Pair<NullWritable, Text>(NullWritable.get(), new Text("20141030 20h16m01s: genero/rock/artista/malta/musica/diz-pra-mim/ouvir")), //
				new Pair<NullWritable, Text>(NullWritable.get(), new Text("20141030 20h16m15s: genero/sertanejo/artista/marcos-e-belutti/musica/domingo-de-manha/ouvir")));
	}

	@Before
	public void setUp() throws Exception {

		final MapeadorDeMusicasOuvidas mapeador = new MapeadorDeMusicasOuvidas();
		final RedutorDeMusicasOuvidas redutor = new RedutorDeMusicasOuvidas();

		driverDeMapeamento = MapDriver.newMapDriver(mapeador);
		driverDeReducao = ReduceDriver.newReduceDriver(redutor);
		driverDeMapeamentoEReducao = MapReduceDriver.newMapReduceDriver(mapeador, redutor);
	}

	@Test
	public void testaMapeamentoComApenasUmRegistro() throws Exception {

		final NullWritable chaveDeEntrada = NullWritable.get();
		final Text valorDeEntrada = new Text("20141030 20h15m55s: genero/pop/artista/clean-bandit-ft-jess-glynne/musica/rather-be/ouvir");

		final Pair<Text, LongWritable> saidaEsperada = new Pair<Text, LongWritable>(new Text("rather-be"), UM);

		driverDeMapeamento.withInput(chaveDeEntrada, valorDeEntrada)//
				.withOutput(saidaEsperada) //
				.runTest();
	}

	@Test
	public void testaMapeamentoComNRegistros() throws Exception {

		final List<Pair<NullWritable, Text>> listaDeEntrada = getListaDeValoresEntrada();

		final List<Pair<Text, LongWritable>> listDeSaidaEsperada = getListaDeMusicasMapeadas();

		driverDeMapeamento.withAll(listaDeEntrada)//
				.withAllOutput(listDeSaidaEsperada) //
				.runTest();
	}

	@Test
	public void testaMapeamentoEReducaoComoTarefaUnica() throws Exception {

		final List<Pair<NullWritable, Text>> listaDeEntrada = getListaDeValoresEntrada();

		final List<Pair<Text, LongWritable>> listaDeSaidaEsperada = Arrays.asList(//
				new Pair<Text, LongWritable>(new Text("rather-be"), UM), //
				new Pair<Text, LongWritable>(new Text("diz-pra-mim"), UM), //
				new Pair<Text, LongWritable>(new Text("domingo-de-manha"), UM));

		driverDeMapeamentoEReducao.withAll(listaDeEntrada)//
				.withAllOutput(listaDeSaidaEsperada)//
				.runTest(ORDEM_NAO_IMPORTA);

	}

	@Test
	public void testaReducaoApenasUmRegistro() throws Exception {

		final Pair<Text, List<LongWritable>> valorDeEntrada = new Pair<Text, List<LongWritable>>(//
				new Text("rather-be"), //
				Arrays.asList(UM, DOIS)//
		);

		final Pair<Text, LongWritable> valorDeSaidaEsperado = new Pair<Text, LongWritable>(//
				new Text("rather-be"), //
				TRES//
		);

		driverDeReducao.withInput(valorDeEntrada)//
				.withOutput(valorDeSaidaEsperado)//
				.runTest();
	}

	@Test
	public void testaReducaoComNRegistros() throws Exception {

		final List<Pair<Text, List<LongWritable>>> listaDeEntrada = Arrays.asList(//
				new Pair<Text, List<LongWritable>>(new Text("rather-be"), Arrays.asList(UM, DOIS)), //
				new Pair<Text, List<LongWritable>>(new Text("diz-pra-mim"), Arrays.asList(DOIS, TRES)), //
				new Pair<Text, List<LongWritable>>(new Text("domingo-de-manha"), Arrays.asList(UM, TRES))//
				);

		final List<Pair<Text, LongWritable>> listaDeSaidaEsperada = Arrays.asList(//
				new Pair<Text, LongWritable>(new Text("rather-be"), TRES), //
				new Pair<Text, LongWritable>(new Text("diz-pra-mim"), CINCO), //
				new Pair<Text, LongWritable>(new Text("domingo-de-manha"), QUATRO)//
				);

		driverDeReducao.withAll(listaDeEntrada)//
				.withAllOutput(listaDeSaidaEsperada)//
				.runTest();
	}
}
