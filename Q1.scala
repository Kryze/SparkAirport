val df = spark.read
      .option("header", "true")
      .csv("datas/year/2006_shuf.csv")

//Permet d'afficher le schéma du fichier CSV
//df.printSchema();

//Ici on remplace NA par 0 pour les Arrivés
df = df.withColumn("ArrDelay", when(col("ArrDelay").equalTo("NA"), 0)
                             .otherwise(col("ArrDelay")));
//Ici on remplace NA par 0 pour les départs
df = df.withColumn("DepDelay", when(col("DepDelay").equalTo("NA"), 0)
                             .otherwise(col("DepDelay")));

// Ici on prends la moyenne des délais d'Arrivée + des délais de Départ pour le jour du mois
val dayOfMonthDelays = df.groupBy("DayOfMonth").agg((avg("ArrDelay")+avg("DepDelay")).as("TotalDelaysDayMonth"))

// Ici on prends la moyenne des délais d'Arrivée + des délais de Départ pour le mois
val monthDelays = df.groupBy("Month").agg((avg("ArrDelay")+avg("DepDelay")).as("TotalDelaysMonth"))

// Ici on prends la moyenne des délais d'Arrivée + des délais de Départ pour le jour de la semaine
val dayOfWeekDelays = df.groupBy("DayOfWeek").agg((avg("ArrDelay")+avg("DepDelay")).as("TotalDelaysDayWeek"))

//On affiche ensuite pour chacun de façon ascendante du plus petit au plus grand délais
dayOfMonthDelays.sort(asc("TotalDelaysDayMonth")).show();
monthDelays.sort(asc("TotalDelaysMonth")).show();
dayOfWeekDelays.sort(asc("TotalDelaysDayWeek")).show();


