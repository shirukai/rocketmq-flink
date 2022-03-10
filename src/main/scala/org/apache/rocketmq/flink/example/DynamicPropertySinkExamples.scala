package org.apache.rocketmq.flink.example

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 动态属性Sink的示例
 *
 * @author shirukai
 */
object DynamicPropertySinkExamples {
  // 订单实体
  case class Order(id: String, user: String, item: String)

  def main(args: Array[String]): Unit = {

    // 1. 创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 创建数据源
    val orders = env.fromElements(
      Order("order-1","张三","iphone 12"),
      Order("order-1","李四","xiaomi 12"),
      Order("order-1","张三","ipad mini"))

    // 3. 通过DataStream创建一个张表
    val orderTable = tableEnv.fromDataStream(orders)

    tableEnv.createTemporaryView("Orders",orderTable)
    tableEnv.executeSql("SELECT * FROM Orders").print()

    // 4. 创建RocketMQ的TableSink
    tableEnv.executeSql(
      """
        |CREATE TABLE rocketmq_sink_with_dynamic_property(
        | `id` string,
        | `user` string,
        | `item` string
        | )WITH(
        | 'connector' = 'rocketmq',
        | 'topic' = 'rocketmq_sink_with_dynamic_property',
        | 'isDynamicProperty' = 'true',
        | 'dynamicPropertyColumns' = 'user,item',
        | 'nameServerAddress' = 'localhost:9876',
        | 'producerGroup' = 'flink'
        | )
        |""".stripMargin)

    // 5. 数据写出
    orderTable.executeInsert("rocketmq_sink_with_dynamic_property").print()

  }
}
