package com.loda.day01wordcount

import org.apache.flink.api.scala._

/**
 * @Author loda
 * @Date 2023/4/8 21:39
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
object Hello02WordCountByDataSet {
	def main(args: Array[String]): Unit = {
		//env
		val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

		//source
		val fileSource: DataSet[String] = environment.readTextFile("data/wordcount.txt")

		//transformations
		val flatMapOp: DataSet[String] = fileSource.flatMap(_.split(" "))
		val mapOp: DataSet[(String, Int)] = flatMapOp.map(_ -> 1)
		val sum: AggregateDataSet[(String, Int)] = mapOp.groupBy(0).sum(1)

		//sink
		sum.print()
	}
}
