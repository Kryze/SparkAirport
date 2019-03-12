import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler

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

val dataset = df.groupBy("UniqueCarrier");

val companyDelays = df.groupBy("UniqueCarrier").agg(avg("ArrDelay"),avg("DepDelay"))

val assembler = new VectorAssembler()
.setInputCols(Array("avg(ArrDelay)", "avg(DepDelay)"))
.setOutputCol("features");

val data = assembler.transform(companyDelays)


val kmeans = new KMeans().setK(5).setSeed(1L)
val model = kmeans.fit(data)

// Make predictions
val predictions = model.transform(data)
predictions.show()

// Shows the result.
//println("Cluster Centers: ")
model.clusterCenters.foreach(println)

predictions.select("UniqueCarrier","prediction").filter($"prediction" ===4).show()


/**val companyDelaysCategory = companyDelays.withColumn("Category",when(col("TotalDelays") < 0 , "Globally in Advance")
                                         .when(col("TotalDelays")< 5, "Globally in Time")
                                         .when(col("TotalDelays")< 10, "Globally with Small Delay")
                                         .when(col("TotalDelays")< 15, "Globally with Delay")
                                         .when(col("TotalDelays")>= 15, "Globally with Long Delay"));**/

//On affiche ensuite pour chacun de façon ascendante du plus petit au plus grand délais
//companyDelays.sort(asc("TotalDelays")).show();
//companyDelaysCategory.show(False);


