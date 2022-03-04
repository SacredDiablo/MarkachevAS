  def salted (df1: DataFrame, df2: DataFrame, df1JoinCol: List[Column], df2JoinCol: List[Column], joinType: String,
              skewedFactor: Int = 3)(skewedMin:Int = 0)(skewedMax: Int = skewedMin * 10) = {

    val t1 = df1.withColumn("conc_df1", functions.concat(df1JoinCol:_*))

    val t1Count = t1.groupBy($"conc_df1").agg(count("*").as("cnt"))

    val avgCnt = t1Count.agg(functions.max($"cnt").as("max"), avg($"cnt").cast(IntegerType).as("avg_cnt"))
      .first()

    val t1Skew = t1Count
      .filter($"cnt" > (
        if(avgCnt.getInt(1) * skewedFactor > skewedMin) avgCnt.getInt(1) * skewedFactor else skewedMin
        )).select($"conc_df1")
      .map(r => r.getString(0)).collect().toList

    val toSalted = t1Skew.toDF("conc_df1")
      .crossJoin(spark.range((avgCnt.getLong(0)/(
        if(avgCnt.getInt(1) * skewedFactor > skewedMin) avgCnt.getInt(1) * skewedFactor else skewedMin
        )).toInt).toDF("salt"))

    val t1Salted = t1.withColumn("salted_df1", when($"conc_df1".isin(t1Skew:_*),
      functions.concat($"conc_df1", lit("-"), (rand()*
        (avgCnt.getLong(0) / (
          if(avgCnt.getInt(1) * skewedFactor > skewedMin) avgCnt.getInt(1) * skewedFactor else skewedMin
          ))).cast(IntegerType)))
      .otherwise($"conc_df1"))
      .repartition((avgCnt.getLong(0)/ (
        if(avgCnt.getInt(1) * skewedFactor > skewedMin) avgCnt.getInt(1) * skewedFactor else skewedMin
        )).toInt, $"salted_df1")

    val t2 = df2.withColumn("conc_df2", functions.concat(df2JoinCol:_*)).as("df2")
      .join(toSalted.as("salt"), $"df2.conc_df2" === $"salt.conc_df1", "left")
      .withColumn("salted_df2", when($"salt".isNull, $"df2.conc_df2")
        .otherwise(functions.concat($"df2.conc_df2", lit("-"), $"salt")))
      .drop("conc_df2", "conc_df1", "salt")

    t1Salted.as("df1").join(t2.as("df2"), $"df1.salted_df1" === $"df2.salted_df2", joinType)
      .drop("df1.conc_df1", "df1.cnt", "df1.salted", "df2.salted", "conc_df1", "salted_df1", "salted_df2")
  }
