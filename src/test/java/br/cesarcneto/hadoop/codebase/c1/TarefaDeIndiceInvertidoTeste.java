package br.cesarcneto.hadoop.codebase.c1;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TarefaDeIndiceInvertidoTeste {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() throws Exception {

		final MapeadorDeIndiceInvertido mapper = new MapeadorDeIndiceInvertido();
		final RedutorDeIndiceInvertido reducer = new RedutorDeIndiceInvertido();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapperWithSingleInputAndMultipleOutput() throws Exception {
		final LongWritable key = new LongWritable(0);
		mapDriver.withInput(key, new Text("www.amazon.com,books,music,toys,ebooks,movies,computers"));
		final List<Pair<Text, Text>> result = mapDriver.run();

		final Pair<Text, Text> books = new Pair<Text, Text>(new Text("books"), new Text("www.amazon.com"));
		final Pair<Text, Text> toys = new Pair<Text, Text>(new Text("toys"), new Text("www.amazon.com"));

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(equalTo(6)));
		assertTrue(result.containsAll(Arrays.asList(books, toys)));
	}

	@Test
	public void testMapperWithSingleKeyAndValue() throws Exception {
		final LongWritable inputKey = new LongWritable(0);
		final Text inputValue = new Text("www.kroger.com,groceries");

		final Text outputKey = new Text("groceries");
		final Text outputValue = new Text("www.kroger.com");

		mapDriver.withInput(inputKey, inputValue);
		mapDriver.withOutput(outputKey, outputValue);
		mapDriver.runTest();
	}

	@Test
	public void testMapperWithSingleKeyAndValueWithAssertion() throws Exception {
		final LongWritable inputKey = new LongWritable(0);
		final Text inputValue = new Text("www.kroger.com,groceries");
		final Text outputKey = new Text("groceries");
		final Text outputValue = new Text("www.kroger.com");

		mapDriver.withInput(inputKey, inputValue);
		final List<Pair<Text, Text>> result = mapDriver.run();

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(equalTo(1)));
		assertThat(result, equalTo(Arrays.asList(new Pair<Text, Text>(outputKey, outputValue))));

	}

	@Test
	public void testMapReduce() throws Exception {
		mapReduceDriver.withInput(new LongWritable(0), new Text("www.kohls.com,clothes,shoes,beauty,toys"));
		mapReduceDriver.withInput(new LongWritable(1), new Text("www.macys.com,shoes,clothes,toys,jeans,sweaters"));

		final List<Pair<Text, Text>> result = mapReduceDriver.run();

		final Pair clothes = new Pair<Text, Text>(new Text("clothes"), new Text("www.kohls.com,www.macys.com"));
		final Pair jeans = new Pair<Text, Text>(new Text("jeans"), new Text("www.macys.com"));

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(equalTo(6)));
		assertTrue(result.containsAll(Arrays.asList(clothes, jeans)));
	}

	@Test
	public void testReducer() throws Exception {
		final Text inputKey = new Text("books");
		final ImmutableList<Text> inputValue = ImmutableList.of(new Text("www.amazon.com"), new Text("www.ebay.com"));

		reduceDriver.withInput(inputKey, inputValue);
		final List<Pair<Text, Text>> result = reduceDriver.run();
		final Pair<Text, Text> pair2 = new Pair<Text, Text>(inputKey, new Text("www.amazon.com,www.ebay.com"));

		assertThat(result, is(notNullValue()));
		assertThat(result.size(), is(equalTo(1)));
		assertThat(result, equalTo(Arrays.asList(pair2)));
	}

}
