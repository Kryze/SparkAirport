val df = spark.read
      .option("header", "true")
      .csv("datas/year/2006_shuf.csv")

//Permet d'afficher le schéma du fichier CSV
//df.printSchema();

// On filtre les colonnes où le delai est supérieur à 0 et on compte

val countCarrierDelay = df.filter($"CarrierDelay">0).count();

val countWeatherDelay = df.filter($"WeatherDelay">0).count();

val countNASDelay = df.filter($"NASDelay">0).count();

val countSecurityDelay = df.filter($"SecurityDelay">0).count();

val countLateAircraftDelay = df.filter($"LateAircraftDelay">0).count();

// On le met dans un DataFrame
val countDelaysReasons = Seq(("CarrierDelay",countCarrierDelay),("WeatherDelay",countWeatherDelay),("NASDelay",countNASDelay),("SecurityDelay",countSecurityDelay),("LateAircraftDelay",countLateAircraftDelay)).toDF("reason","count")

//On affiche de façon ascendante
countDelaysReasons.sort(asc("count")).show();


