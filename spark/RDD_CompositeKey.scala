package spark.tasks

import org.apache.spark.{SparkConf, SparkContext}
import spark.tasks.domain.{Customer, Order, Product}

object Lab4CompositeKey extends App {
  /*
 * Lab4 - пример использования cartesian и join по составному ключу
 * Расчитать кто и на какую сумму купил определенного товара за всё время
 * Итоговое множество содержит поля: customer.name, product.name, order.numberOfProduct * product.price
 *
 * 1. Создать экземпляр класса SparkConf+
 * 2. Установить мастера на local[*] и установить имя приложения+
 * 3. Создать экземпляр класса SparkContext, используя объект SparkConf+
 * 4. Загрузить в RDD файлы src/test/resources/input/order+
 * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить RDD+
 * 6. Выбрать ключ (customerID, productID), значение (numberOfProduct)+
 * 7. Загрузить в RDD файлы src/test/resources/input/product+
 * 8. Используя класс [[ru.phil_it.bigdata.entity.Product]], распарсить RDD+
 * 9. Загрузить в RDD файлы src/test/resources/input/customer+
 * 10. Используя класс [[ru.phil_it.bigdata.entity.Customer]], распарсить RDD+
 * 11. Выполнить перекрестное соединение RDD из п.8 и п.10+
 * 12. Выбрать ключ (customer.id, product.id), значение (customer.name, product.name, prodcut.price)+
 * 13. Выполнить левое соединение RDD из п.6 и п.13
 * 14. Поставить заглушку на результат соединения для левой таблицы ("default", "default", 0d)
 * 15. Выбрать поля custemer.name, product.name, order.numberOfProduct * product.price
 * 16. Вывести результат или записать в директорию src/test/resources/output/lab4
 * */
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("lab_3")
  val sc = new SparkContext(sparkConf)

  val order = sc.textFile("C:\\Users\\SacredDiablo\\IdeaProjects\\Labs1\\dataset\\order\\*")
    .map(line => Order(line))
    .map(order => ((order.customerID, order.productID), order.numberOfProduct))
  val product = sc.textFile("C:\\Users\\SacredDiablo\\IdeaProjects\\Labs1\\dataset\\product\\*")
    .map(line => Product(line))
  val customer = sc.textFile("C:\\Users\\SacredDiablo\\IdeaProjects\\Labs1\\dataset\\customer\\*")
    .map(line => Customer(line))

  val cart = product.cartesian(customer)
    .map{
      case(product, customer) => ((product.id, customer.id), (product.name, customer.name, product.price))
    }
  val leftouter = order.leftOuterJoin(cart)
    .map{
      case (key, (order, cart)) => (key, (order, cart. getOrElse("default", "default", 0d)))
    }
  val pole = leftouter.map {
    case ((customerId, productId), (orderProductNumber, (customerName, productName, productPrice))) =>
      (customerName ::
        productName ::
        orderProductNumber * productPrice ::
        Nil).mkString("\t")
  }.foreach(println(_))
}
