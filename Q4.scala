val df = spark.read
      .option("header", "true")
      .csv("datas/year/2006_shuf.csv")

val dfairports = spark.read
      .option("header", "true")
      .csv("datas/airports.csv")

//Permet d'afficher le schéma du fichier CSV
//df.printSchema();

//Ici on remplace NA par 0 pour les Arrivés
df = df.withColumn("ArrDelay", when(col("ArrDelay").equalTo("NA"), 0)
                             .otherwise(col("ArrDelay")));
//Ici on remplace NA par 0 pour les départs
df = df.withColumn("DepDelay", when(col("DepDelay").equalTo("NA"), 0)
                             .otherwise(col("DepDelay")));

val airportDelays = df.groupBy("Origin").agg((avg("ArrDelay")+avg("DepDelay")).as("TotalDelaysAirport"))
                      .orderBy($"TotalDelaysAirport".desc).limit(3)

val airportNameDelays = airportDelays.as("df1").join(dfairports.as("df2"), airportDelays("Origin") === dfairports("iata")).select("df1.Origin", "df2.airport", "df1.TotalDelaysAirport")

airportNameDelays.show(false)