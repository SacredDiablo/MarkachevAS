package spark.tasks

import org.apache.spark.{SparkConf, SparkContext}
import spark.tasks.domain.Order

object Lab6AggregateBy extends App {
  /*
   * Lab6 - пример использования aggregateByKey
   * Определить кол-во уникальных заказов, максимальный объем заказа,
   * минимальный объем заказа, общий объем заказа за всё время
   * Итоговое множество содержит поля: order.customerID, count(distinct order.productID),
   * max(order.numberOfProduct), min(order.numberOfProduct), sum(order.numberOfProduct)
   *
   * 1. Создать экземпляр класса SparkConf+
   * 2. Установить мастера на local[*] и установить имя приложения+
   * 3. Создать экземпляр класса SparkContext, используя объект SparkConf+
   * 4. Загрузить в RDD файд src/test/resources/input/order+
   * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить строки в RDD+
   * 6. Выбрать только те транзакции у которых статус delivered+
   * 7. Выбрать ключ (customerID), значение (productID, numberOfProducts)+
   * 8. Создать кейс класс Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)+
   * 9. Создать аккумулятор с начальным значением
   * 10. Создать ананимную функцию для заполнения аккумульятора
   * 11. Создать ананимную функцию для слияния аккумуляторов
   * 12. Выбрать id заказчика, размер коллекции uniqProducts,
   *   максимальное и минимальное значение из uniqNumOfProducts и sumNumOfProduct
   * 13. Вывести результат или записать в директорию src/test/resources/output/lab6
   * */
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("lab_3")
  val sc = new SparkContext(sparkConf)

  val order = sc.textFile("C:\\Users\\SacredDiablo\\IdeaProjects\\Labs1\\dataset\\order\\*")
    .map(line => Order(line))
    .filter(order => order.status.equals("delivered"))
    .map(order => (order.customerID,(order.productID, order.numberOfProduct)))

  case class Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)

  val agg = order.aggregateByKey(Result(Set.empty[Int], Seq.empty[Int], 0))((acc, order) =>{
    val uniq = acc.uniqProducts + order._1
    val uniqNum = acc.uniqNumOfProducts :+ order._2
    Result(uniq, uniqNum, acc.sumNumOfProduct + order._2)
  } , (one, two) => {
    Result(one.uniqProducts ++ two.uniqProducts,
      one.uniqNumOfProducts ++ two.uniqNumOfProducts,
      one.sumNumOfProduct + two.sumNumOfProduct)
  })
    .map{
      case (key, result) =>
        s"$key\t${result.uniqProducts.size}\t${result.uniqNumOfProducts.max}\t${result.uniqNumOfProducts.min}\t${result.sumNumOfProduct}"
    }.foreach(println(_))
}
