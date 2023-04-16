package com.loda.day01wordcount

import org.apache.flink.streaming.api.scala._

/**
 * @Author loda
 * @Date 2023/4/8 21:58
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello04WordCountByDataStream {
	def main(args: Array[String]): Unit = {
		// env
		val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//source
		val socketStream: DataStream[String] = environment.socketTextStream("localhost", 9999)

		//transformations
		val flatMapOp: DataStream[String] = socketStream.flatMap(_.split(" "))
		val mapOp: DataStream[(String, Int)] = flatMapOp.map(_ -> 1)
		val sum: DataStream[(String, Int)] = mapOp.keyBy(_._1).sum(1)
//		val sum: DataStream[(String, Int)] = mapOp.keyBy(0).sum(1)

		//sink
		sum.print()

		//
		environment.execute();
	}
}
