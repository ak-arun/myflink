package com.ak.examples.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ak.examples.flink.util.LineTokenizer;

public class SocketTextStreamWordCount {

	

	public static void main(String[] args) throws Exception {

		if (args.length != 2){
			System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
			return;
		}

		String hostName = args[0];
		Integer port = Integer.parseInt(args[1]);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		DataStream<String> text = env.socketTextStream(hostName, port);

		DataStream<Tuple2<String, Integer>> counts =
		text.flatMap(new LineTokenizer())
				.keyBy(0)
				.sum(1);

		counts.print();

		env.execute("Java WordCount from SocketTextStream Example");
	}

	
}
