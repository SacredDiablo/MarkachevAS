import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import training.system.Parameters


object Lab6 extends App {
  /*
   * Lab11 - пример использования оконных выражений
   * Необходимо определить самый популярный продукт у клиента
   * Итоговое множество содержит поля: customer.name, product.name
   *
   * +1. Создать неявный объект SparkSession, используя метод builder()
   * +2. Установить master в local[*]
   * +3. Установить имя приложения spark-sql-labs
   * +4. Создать неявный объект класса Parameters, используя метод Parameters.instance
   * +5. Вызвать метод initTables класса Parameters
   * +6. Загрузить таблицу product в DataFrame
   * +7. Выбрать поля: id далее product_id, name далее product_name
   * +8. Загрузить таблицу customer в DataFrame
   * +9. Выбрать поля: id далее customer_id, name далее customer_name
   * +10. Выполнить перекрестное соединение DataFrame из п.7 и п.9
   * +11. Загрузить таблицу order в DataFrame
   * +12. Выполнить группировку по полю customer_id, product_id
   * +13. Расчитать сумму по полю number_of_product, далее sum_num_of_product
   * +14. Добавить import org.apache.spark.sql.expressions.Window
   * +15. Написать оконную функцию с партицирование по полю customer_id
   * и сортирвокой в порядке убывания по полю sum_num_of_product
   * +16. Использую аналитичексую функцию row_number и оконное выражение из п.15
   * добавить поле rn
   * +17. Выбрать только те строки, в которых значение поля rn = 1
   * 18. Выполнить внутреннее соединение DataFrame из п.17 и п.10 по полям:
   * customer_id, product_id
   * 19. Выбрать поля customer_name, product_name
   * 20. Вывести результат используя метод show() или записать DataFrame в файл
   * */
  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark-sql-labs")
    .enableHiveSupport()
    .getOrCreate()

  Parameters.initTables

  val df = spark.table("product")
    .select(col("id").as("product_id")
      ,col("name").as("product_name")
    )
  val df2 = spark.table("customer")
    .select(col("id").as("customer_id")
      ,col("name").as("customer_name")
    )

  val result_df = df.crossJoin(df2)

  val df3 = spark.table("order")
  df3.show()
  val windowBySum = Window.partitionBy(col("customer_id")).orderBy(col("sum_num_of_product").desc)

  val result_order = df3
    .groupBy(col("customer_id"), col("product_id"))
    .agg(sum(col("number_of_product")) alias "sum_num_of_product")
    .select(
      col("customer_id")
      ,col("product_id")
      ,row_number().over(windowBySum) alias "rn")
    .filter(col("rn") === 1)

  val end_result = result_df.join(result_order, Seq("customer_id", "product_id"), "inner")

  end_result.show()


}
