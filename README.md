# Flink写RocketMQ支持动态UserProperty

> Flink version: 1.14.0
>
> RocketMQ version:  4.5.2
>
> Github: https://github.com/shirukai/rocketmq-flink.git
>
> 本篇文章主要记录了在Flink Table中如何使用RocketMQ的Sink，并且通过修改源码支持动态的UserProperty，写这篇文章就当是补充学习吧，其中涉及到Flink Table自定义数据源、RocketMQ的使用等相关的知识。

# 1. 原由

RocketMQ是apache开源的分布式消息中间件，官网：https://rocketmq.apache.org/。它支持按照多个Tag进行过滤消费，但奇怪的是，它的Java API中并不支持多Tag进行生产消息，我在它的github中也看到了一条与之相关的[issues](https://github.com/apache/rocketmq/issues/2454)，如下图所示：

![image-20220310154545274](https://cdn.jsdelivr.net/gh/shirukai/images/20220310154550.png)

有人回复的很明确，如果想在一条消息中使用多个Tag，可以尝试使用属性过滤器，即通过给message设置多个UserProperty来实现多个条件过滤，所以如果我们使用RocketMQ的Java客户端去生成数据的话，只要对数据设置多个UserProperty就行了 ，但是在开源的flink-connector（https://github.com/apache/rocketmq-flink）里的TableSink是不支持设置UserProperty，所以本文就是来记录一下如何修改rocketmq-flink的代码来支持动态的UserProperty。

# 2 实现

首先说一下讲一下技术原理，其实如果之前看过或实现过自定义Table Sinks的同学来说很容易实现，没接触过的也没事，可以先看看官网这部分的内容：https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sourcessinks/。主要有两个比较关键的类，一个是实现DynamicTableSinkFactory接口的工厂类：用来告诉Flink如何创建Sink实例，以及获取自定义Sink的名称，初始化参数等，第二个是实现DynamicTableSink接口的Sink类，这个类是Sink的具体逻辑。这块原理很简单，Flink Runtime会通过SPI，动态加载对应的Factory实现类，并通过它创建用户自定义的Sink。在本项目中，我们也是主要关心这两个大类即可：RocketMQDynamicTableSink和RocketMQDynamicTableSinkFactory。实现步骤：

1. 定义参数
2. 从上下文中解析参数
3. 根据指定的参数设置UserProperty

## 2.1 定义参数

定义参数，主要是为了让用户在Table API或者SQL中使用Sink的时候，能够灵活的控制UserProperty的启用以及按照哪些列生成属性。

这里定义两个参数：

isDynamicProperty：是否启用动态的UserProperty
dynamicPropertyColumns：生成UserProperty的列名，用逗号分隔

修改org.apache.rocketmq.flink.common.RocketMQOptions类，添加如下代码：

```java
    // NOTE: To support dynamic `UserProperty`
    /**
     *
     * Add some options to support dynamic `UserProperty`
     * <p>
     * isDynamicProperty 是否启用动态属性，默认不开启
     * dynamicPropertyColumns 启动动态属性的列，可以指定多列，以逗号分隔，如 "columnA,columnB"
     */
    public static final ConfigOption<Boolean> OPTIONAL_WRITE_IS_DYNAMIC_PROPERTY = ConfigOptions.key("isDynamicProperty").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_WRITE_DYNAMIC_PROPERTY_COLUMNS = ConfigOptions.key("dynamicPropertyColumns").stringType().noDefaultValue();
```

修改org.apache.rocketmq.flink.sink.tableRocketMQDynamicTableSink类，添加成员变量，并修改构造方法：

```java
    /**
     * Add some fields to support dynamic `UserProperty`
     */
    private final boolean isDynamicProperty;
    private final String[] dynamicPropertyColumns;
    
        public RocketMQDynamicTableSink(
            DescriptorProperties properties,
            TableSchema schema,
            String topic,
            String producerGroup,
            String nameServerAddress,
            String tag,
            String dynamicColumn,
            String fieldDelimiter,
            String encoding,
            long retryTimes,
            long sleepTime,
            boolean isDynamicTag,
            boolean isDynamicTagIncluded,
            boolean writeKeysToBody,
            String[] keyColumns,
            // NOTE: To support dynamic `UserProperty`
            boolean isDynamicProperty,
            String[] dynamicPropertyColumns
    ) {
        this.properties = properties;
        this.schema = schema;
        this.topic = topic;
        this.producerGroup = producerGroup;
        this.nameServerAddress = nameServerAddress;
        this.tag = tag;
        this.dynamicColumn = dynamicColumn;
        this.fieldDelimiter = fieldDelimiter;
        this.encoding = encoding;
        this.retryTimes = retryTimes;
        this.sleepTime = sleepTime;
        this.isDynamicTag = isDynamicTag;
        this.isDynamicTagIncluded = isDynamicTagIncluded;
        this.writeKeysToBody = writeKeysToBody;
        this.keyColumns = keyColumns;
        this.metadataKeys = Collections.emptyList();

        // NOTE: To support dynamic `UserProperty`
        this.isDynamicProperty = isDynamicProperty;
        this.dynamicPropertyColumns = dynamicPropertyColumns;
    }
```

同理修改org.apache.rocketmq.flink.sink.table.RocketMQRowDataConverter类

```java
    /**
     * Add some fields to support dynamic `UserProperty`
     */
    private final boolean isDynamicProperty;
    private final String[] dynamicPropertyColumns;
    // 属性列的索引
    private int[] propertyFieldIndexes;

    public RocketMQRowDataConverter(String topic, String tag, String dynamicColumn, String fieldDelimiter, String encoding, boolean isDynamicTag, boolean isDynamicTagIncluded, boolean writeKeysToBody, String[] keyColumns, RowTypeInfo rowTypeInfo, DataType[] fieldDataTypes, boolean hasMetadata, int[] metadataPositions,

                                    // NOTE: To support dynamic `UserProperty`
                                    boolean isDynamicProperty, String[] dynamicPropertyColumns) {
        this.topic = topic;
        this.tag = tag;
        this.dynamicColumn = dynamicColumn;
        this.fieldDelimiter = fieldDelimiter;
        this.encoding = encoding;
        this.isDynamicTag = isDynamicTag;
        this.isDynamicTagIncluded = isDynamicTagIncluded;
        this.writeKeysToBody = writeKeysToBody;
        this.keyColumns = keyColumns;
        this.rowTypeInfo = rowTypeInfo;
        this.fieldDataTypes = fieldDataTypes;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;

        // NOTE: To support dynamic `UserProperty`
        this.isDynamicProperty = isDynamicProperty;
        this.dynamicPropertyColumns = dynamicPropertyColumns;
    }
```

## 2.2 从上下文中解析参数

如下是一个创建Sink的SQL，WITH里是用户指定的参数，程序中读取解析参数是在RocketMQDynamicTableSinkFactory类里实现的。

```sql
CREATE TABLE rocketmq_sink_with_dynamic_property(
 `id` string,
 `user` string,
 `item` string
 )WITH(
 'connector' = 'rocketmq',
 'topic' = 'rocketmq_sink_with_dynamic_property',
 'isDynamicProperty' = 'true',
 'dynamicPropertyColumns' = 'user,item',
 'nameServerAddress' = 'localhost:9876',
 'producerGroup' = 'flink'
 )
```

这个类里有三个重写方法需要关注：

1. requiredOptions() 该方法告诉Flink哪些参数是必填参数，因为我们定义的这个参数是非必填的，这个地方不用修改，但是要理解，以后如果要新增必填参数的话，需要修改这部分的逻辑。

2. optionalOptions()该方法告诉Flink哪些参数是非必填的，我们需要修改这个部分的逻辑

   ```java
          // NOTE: To support dynamic `UserProperty`
           optionalOptions.add(OPTIONAL_WRITE_IS_DYNAMIC_PROPERTY);
           optionalOptions.add(OPTIONAL_WRITE_DYNAMIC_PROPERTY_COLUMNS);
   ```



3. createDynamicTableSink(Context context)，这个方法比较关键，是用来创建TableSink的，同时我们需要从上下文中解析出对应的参数，然后传给Sink

   ```java
           // NOTE: To support dynamic `UserProperty`
           boolean isDynamicProperty = properties.getBoolean(OPTIONAL_WRITE_IS_DYNAMIC_PROPERTY);
           String dynamicPropertyColumnsConfig = properties.getString(OPTIONAL_WRITE_DYNAMIC_PROPERTY_COLUMNS);
           String[] dynamicPropertyColumns = new String[0];
           if(dynamicPropertyColumnsConfig !=null && dynamicPropertyColumnsConfig.length()>0){
               dynamicPropertyColumns = dynamicPropertyColumnsConfig.split(",");
           }
   
   
           DescriptorProperties descriptorProperties = new DescriptorProperties();
           descriptorProperties.putProperties(rawProperties);
           TableSchema physicalSchema =
                   TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
           return new RocketMQDynamicTableSink(
                   descriptorProperties,
                   physicalSchema,
                   topicName,
                   producerGroup,
                   nameServerAddress,
                   tag,
                   dynamicColumn,
                   fieldDelimiter,
                   encoding,
                   sleepTimeMs,
                   retryTimes,
                   isDynamicTag,
                   isDynamicTagIncluded,
                   writeKeysToBody,
                   keyColumns,
   
                   // NOTE: To support dynamic `UserProperty`
                   isDynamicProperty,
                   dynamicPropertyColumns
           );
   ```



## 2.3 根据指定的参数设置UserProperty

这部分是实现的关键，需要修改RocketMQRowDataConverter这个类，有方法需要关注：

1. open()方法，这个方法是用来初始化的，我们需要在这个方法里，根据用户指定的列名信息，来获取到对应schema中的索引，并将索引保存到成员变量propertyFieldIndexes中

   ```java
           // NOTE: To support dynamic `UserProperty`
           // 根据用户定义的dynamicPropertyColumns属性，获取对应列名的索引
           if (isDynamicProperty && dynamicPropertyColumns != null) {
               propertyFieldIndexes = new int[dynamicPropertyColumns.length];
               for (int i = 0; i < dynamicPropertyColumns.length; i++) {
                   final String column = dynamicPropertyColumns[i];
                   // 获取索引
                   int fieldIndex = rowTypeInfo.getFieldIndex(column);
                   checkState(fieldIndex >= 0, String.format("[MetaQConverter] Could not find the property column: %s.", column));
                   propertyFieldIndexes[i] = fieldIndex;
   
                   // TODO: 这里的逻辑是将设置为UserProperty的列从Body中移除
                   //  如果想要在Body中同样包含对应的列，需要将如下代码移除，或者参考Tag部分代码，设置一个开关量
                   excludedFields.add(fieldIndex);
               }
           }
   				//
           bodyFieldIndexes = new int[rowTypeInfo.getArity() - excludedFields.size()];
           bodyFieldTypes = new DataType[rowTypeInfo.getArity() - excludedFields.size()];
           int index = 0;
           for (int num = 0; num < rowTypeInfo.getArity(); num++) {
               if (!excludedFields.contains(num)) {
                   bodyFieldIndexes[index] = num;
                   bodyFieldTypes[index++] = fieldDataTypes[num];
               }
           }
   
   ```



2. convert()方法，这个方法是将Row转换成RocketMQ的Message的过程，也是我们去动态设置UserProperty的过程

   ```java
           // NOTE: To support dynamic `UserProperty`
           if (isDynamicProperty) {
               checkState(propertyFieldIndexes.length > 0, "No message property column set.");
               for (int i = 0; i < dynamicPropertyColumns.length; i++) {
                   String propertyName = dynamicPropertyColumns[i];
                   int index = propertyFieldIndexes[i];
                   String propertyValue = row.getString(index).toString();
                   // 设置用户属性
                   message.putUserProperty(propertyName, propertyValue);
               }
           }
   
   ```



# 3 测试

通过上面代码的修改，就可以支持动态设置UserProperty了，下面进行测试阶段。

## 3.1 环境准备

这里为了方便，直接使用docker-compose运行一个RocketMQ实例，具体教程可以参考：https://cloud.tencent.com/developer/article/1621263。其中的docker-compose我已经提交到代码仓库了，down下代码来可以直接使用。

```
# 进入项目的docker目录
cd doccker
# 启动
docker-compose up
```

![flink-rocketmq-1](https://cdn.jsdelivr.net/gh/shirukai/images/20220310171140.gif)

运行成功之后，访问localhost:8080，可以进入RocketMQ的控制台

![image-20220310171359388](https://cdn.jsdelivr.net/gh/shirukai/images/20220310171359.png)

## 3.2 编写FlinkJob使用RocketMQ的TableSink写数据

这里为了方便造数和写SQL用Scala写了，其实代码量也差不多。首先创建了DataStream，然后通过它来创建一个OrderTable，在创建一个TableSink，将OrderTable的数据写入TableSink中。

```scala
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

```

## 3.3 编写消费者，并使用属性过滤器进行数据消费

没啥东西，简单写个消费者就行

```java
package org.apache.rocketmq.flink.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * 根据属性过滤的示例
 *
 * @author shirukai
 */
public class PropertyFilterExamples {
    private static final String ROCKETMQ_PRODUCER_TOPIC = "rocketmq_sink_with_dynamic_property";

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("property-filter-examples-1");
        // 指定从头消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe(ROCKETMQ_PRODUCER_TOPIC,MessageSelector.bySql("item = 'iphone 12'"));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("消费消息：body=" + new String(msg.getBody())+",user="+msg.getUserProperty("user")+", item="+msg.getUserProperty("item"));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.currentThread().join();
    }
}
```

## 3.2 验证结果

首先启动消费者程序，在启动Flink程序，这两个程序直接执行man方法运行即可。

![flink-rocketmq-2](https://cdn.jsdelivr.net/gh/shirukai/images/20220310172450.gif)

# 4 总结

代码量不是很多，使用过TableAPI的实现起来相对来说比较简单，通过本次实战，从0到1了解了RocketMQ，以为之前并没有接触过，另外对自定义Table数据源有了跟深入的理解。代码已经提交到Github上了，感兴趣的同学可以一块交流。