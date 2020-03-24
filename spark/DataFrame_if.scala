import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, when}
import shapeless.ops.nat.GT.>
import training.Lab1.spark
import training.domain.{Customer, Product}
import training.system.Parameters

object Lab3 extends App {
  /*
   * Lab8 - пример обработки полей при помощи select/withColumns
   * Вывести название всех устройств,
   * если цена больее 50000 вычесть 10% от стоимости назвать поле new_price,
   * добавить поле type, используя функцию getTypeDevice
   * Итоговое множество содержит поля: product.name, new_price, type
   *
   * +1. Создать неявный объект SparkSession, используя метод builder()
   * +2. Установить master в local[*]
   * +3. Установить имя приложения spark-sql-labs
   * -4. Создать неявный объект класса Parameters, используя метод Parameters.instance
   * +5. Вызвать метод initTables класса Parameters
   * +6. Загрузить таблицу product в DataFrame
   *
   *
   * +7. В import udf добавить так же when и col из того же пакета
   * +8. В методе select() выбрать поле name, используя функцию when().otherwise()
   * расчитать новую стоимость девайса
   * 9. Используя метод withColumn и функцию getTypeDevice, добавить к выборке поле type
   * 10. Вывести результат используя метод show() или записать DataFrame в файл
   *
   *
   * */
  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark-sql-labs")
    .enableHiveSupport()
    .getOrCreate()

  spark.read.format("csv")
    .option("nullValue", "NULL")
    .option("delimiter", "\t")
    .schema(Product.structType)
    .load("C:\\Users\\User\\Desktop\\Новая папка (2)\\Labs2\\dataset\\product\\*")
    .createTempView("customer")

  Parameters.initTables

  val df = spark.table("product")
  df.select(col("name")
    , col("price")
    , when(col("price") > 50000,col("price")*0.9)
      .otherwise(col("price")).as("new_price")).show()

  private val getTypeDevice = udf((nameDevice:String) => {
    val lcaseDeviceName = nameDevice.toLowerCase()
    if(lcaseDeviceName.contains("iphone"))
      "SMARTPHONE"
    else if (lcaseDeviceName.contains("ipad"))
      "TABLET"
    else if (lcaseDeviceName.contains("macbook"))
      "LAPTOP"
    else "ACCESSORY"
  })


  df.withColumn("type", getTypeDevice(col("name"))).show()

}
