package com.loda.wordcount

import org.apache.flink.streaming.api.scala._

/**
 * @Author loda
 * @Date 2023/4/8 21:58
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello05WordCountByDataStream {
	def main(args: Array[String]): Unit = {
		// env
		val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//source
		val socketStream: DataStream[String] = environment.socketTextStream("localhost", 9999)

		//transformations
		val sum: DataStream[(String, Int)] = socketStream.flatMap(_.split(" "))
					.map(_ -> 1).keyBy(_._1).sum(1)

		//sink
		sum.print()

		//
		environment.execute();
	}
}
