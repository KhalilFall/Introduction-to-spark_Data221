// Databricks notebook source
// MAGIC %md 
// MAGIC # Introduction à Apache Spark

// COMMAND ----------

// MAGIC %md
// MAGIC Nous nous contenterons exclusivement de travailler avec les DataFrame, sachant qu'ils sont plus simples et plus familiers
// MAGIC 
// MAGIC > Données Titanic : https://www.kaggle.com/c/titanic/data

// COMMAND ----------

// MAGIC %md
// MAGIC > Description des colonnes
// MAGIC 
// MAGIC <table>
// MAGIC     <tr>
// MAGIC       <td>Variable</td>
// MAGIC       <td>Definition</td>
// MAGIC       <td>Key</td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>survival</td>
// MAGIC       <td>Survival</td>
// MAGIC       <td>0 = No, 1 = Yes</td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>pclass</td>
// MAGIC       <td>Ticket class</td>
// MAGIC       <td>1 = 1st, 2 = 2nd, 3 = 3rd</td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>Sex</td>
// MAGIC       <td>Sex</td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>Age</td>
// MAGIC       <td>Age in years</td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>sibsp</td>
// MAGIC       <td># of siblings / spouses aboard the Titanic</td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>parch</td>
// MAGIC       <td># of parents / children aboard the Titanic </td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>ticket</td>
// MAGIC       <td>Ticket number</td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>fare</td>
// MAGIC       <td>Passenger fare</td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>cabin</td>
// MAGIC       <td>Cabin number</td>
// MAGIC       <td></td>
// MAGIC     </tr>
// MAGIC   <tr>
// MAGIC       <td>embarked</td>
// MAGIC       <td>Port of Embarkation</td>
// MAGIC       <td>C = Cherbourg, Q = Queenstown, S = Southampton</td>
// MAGIC     </tr>
// MAGIC </table>

// COMMAND ----------

spark

// COMMAND ----------

//Créer un sparkSession
import org.apache.spark.sql.SparkSession
val sparkSess = SparkSession.builder.master("local[*]").appName("titanic").getOrCreate()

// COMMAND ----------

// MAGIC %md ### Les RDD (Distributed Collection)

// COMMAND ----------

// MAGIC %md ####Créer un RDD

// COMMAND ----------

// MAGIC %md ###### A partir d'une collection

// COMMAND ----------

//Grace à une collection
val clientCollection = List(("fall", "M", 1000), ("momar","M", 380), ("fatou", "F", 500),("noumbé", "F", 390))
val clientRDD         = spark.sparkContext.parallelize(clientCollection)

// COMMAND ----------

clientDF.take(10)

// COMMAND ----------

// MAGIC %md ###### A partir d'une source externe

// COMMAND ----------

val dataRDD = spark.sparkContext.textFile("/FileStore/tables/titanic.txt")

// COMMAND ----------

dataRDD.take(3)

// COMMAND ----------

dataRDD.take(2)

// COMMAND ----------

// MAGIC %md ###### A partir d'un autre RDD

// COMMAND ----------

val femaleData = dataRDD.filter(line => line.split(",")(5) == "female")
femaleData.take(5)

// COMMAND ----------

// MAGIC %md #### Quelques traitements avec les RDD

// COMMAND ----------

// MAGIC %md ###### Supprimer la première ligne (entête)

// COMMAND ----------

val header          = dataRDD.take(1).mkString
val dfWithoutHeader = dataRDD.filter(line => line != header)
dfWithoutHeader.take(3)

// COMMAND ----------

// MAGIC %md ###### Quelques traitements

// COMMAND ----------

val mapSimple = dfWithoutHeader.map(line => line.split(","))
mapSimple.take(5)

// COMMAND ----------

val mapSimple = dfWithoutHeader.flatMap(line => line.split(","))
mapSimple.take(5)

// COMMAND ----------

//Valeur distinct
val distinctSex  = dfWithoutHeader.map(line => line.split(",")(5)).distinct
distinctSex.take(5)

// COMMAND ----------

// MAGIC %md ###### Dead Children

// COMMAND ----------

val children = dfWithoutHeader.map(line => line.split(",")(8)).countByValue
val deadChildren = dfWithoutHeader.filter(line => line.split(",")(8) != "0")
val groupedDeadChildren =deadChildren.map(line => line.split(",")(5)).countByValue

// COMMAND ----------

val deadMaleChildren = dfWithoutHeader.filter(line => (line.split(",")(8) != "0") && (line.split(",")(5) == "male")).count

// COMMAND ----------

val ApproxDistinctSex = dfWithoutHeader.map(line => line.split(",")(5)).countApproxDistinct(0.05)
val nombreMale        = dfWithoutHeader.filter(line => line.split(",")(5) == "male").count
val countSurvived     = dfWithoutHeader.map(line => line.split(",")(1)).countByValue
val sexInSurvived     = dfWithoutHeader.filter(line => line.split(",")(1) == "1").map(line => line.split(",")(5)).countByValue
val sexInDead         = dfWithoutHeader.filter(line => line.split(",")(1) == "0").map(line => line.split(",")(5)).countByValue

// COMMAND ----------

// MAGIC %md ###### Aggréger les données

// COMMAND ----------

// Somme des ages

val ageTotal = dfWithoutHeader.map(line => {val splittedLine = line.split(",")(6); if(splittedLine == "") 0 else splittedLine.toFloat}).reduce(_ + _)
ageTotal

// COMMAND ----------

// Nombre total de survivants par ticketclass

val groupedDF = dfWithoutHeader.map(line => (line.split(",")(2), line.split(",")(1).toInt)).groupByKey
val survivantsParClass = groupedDF.map(tup => (tup._1, tup._2.toList.sum))
survivantsParClass.take(5).foreach(println)

// COMMAND ----------

// Nombre total de survivants par ticketclass with reduceByKey
val tupleDF = dfWithoutHeader.map(line => (line.split(",")(2), line.split(",")(1).toInt))
val survivantsParClass2 = tupleDF.reduceByKey((total, value) => total + value)
survivantsParClass2.take(5).foreach(println)

// COMMAND ----------

// Le plus agé des survivants et ds morts
val tupleSurvivant = dfWithoutHeader.map(line => (line.split(",")(1), if(line.split(",")(6) == "") 0 else line.split(",")(6).toFloat))
val lePlusVieuxSurvivnat  = tupleSurvivant.reduceByKey((val1, val2) => if(val1 > val2) val1 else val2)

lePlusVieuxSurvivnat.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md ###### Ranking

// COMMAND ----------

// Ordonner les donnnées par age

dfWithoutHeader.filter(line => line.split(",")(6) != "")
               .takeOrdered(10)(Ordering[Float].reverse.on(line => line.split(",")(6).toFloat)).foreach(println)

// COMMAND ----------

// MAGIC %md #### Créer un DataFrame

// COMMAND ----------

// MAGIC %md ##### A partir d'un RDD

// COMMAND ----------

dataRDD.take(3)

// COMMAND ----------

dfWithoutHeader.map(rawToTuple(_)).take(5)

// COMMAND ----------

def rawToTuple(line: String) = {
  val splittedLine = line.split(",")
  
  (
    splittedLine(0), 
    splittedLine(1), 
    splittedLine(2), 
    splittedLine(3), 
    splittedLine(4), 
    splittedLine(5), 
    splittedLine(6), 
    splittedLine(7), 
    splittedLine(8), 
    splittedLine(9), 
    splittedLine(10), 
    splittedLine(11), 
    splittedLine(12)
  )
}

val df = spark.createDataFrame(dfWithoutHeader.map(rawToTuple(_))).toDF("PassengerId","Survived","Pclass","Name","Firstname","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked")

df.show()

// COMMAND ----------

// MAGIC %md ##### A partir d'une collection

// COMMAND ----------

val clientCollection = List(("fall", "M", 1000), ("momar","M", 380), ("fatou", "F", 500),("noumbé", "F", 390))
val clientDF         = clientCollection.toDF("nom", "sex", "solde")
clientDF.show()

// COMMAND ----------

// MAGIC %md ##### A partir d'un fichier externe

// COMMAND ----------

val file_location = "/FileStore/tables/titanic.csv"
val file_type = "csv"

val infer_schema = "true"
val first_row_is_header = "true"
val delimiter = ","

val fullDF = spark.read.format(file_type) 
              .option("inferSchema", infer_schema) 
              .option("header", first_row_is_header) 
              .option("sep", delimiter) 
              .load(file_location)

display(fullDF)

// COMMAND ----------

fullDF.printSchema

// COMMAND ----------

// MAGIC %md ##### A partir d'un fichier externe et un schéma prédéfini

// COMMAND ----------

import org.apache.spark.sql.types._

val file_location = "/FileStore/tables/titanic.csv"
val file_type = "csv"

val mySchema = StructType(Array(
                                StructField("PassengerId", StringType, true),
                                StructField("Survived", StringType, true),
                                StructField("Pclass", StringType, true),
                                StructField("Name", StringType, true),
                                StructField("Sex", StringType, true),
                                StructField("Age", IntegerType, true),
                                StructField("SibSp", IntegerType, true),
                                StructField("Parch", IntegerType, true),
                                StructField("Ticket", StringType, true),
                                StructField("Fare", DoubleType, true),
                                StructField("Cabin", StringType, true),
                                StructField("Embarked", StringType, true)
                          ))
val first_row_is_header = "true"
val delimiter = ","

val fullDF = spark.read.format(file_type) 
              .option("header", first_row_is_header) 
              .option("sep", delimiter) 
              .schema(mySchema)
              .load(file_location)

display(fullDF)

// COMMAND ----------

// MAGIC %md #### Quelques traitements avec les DF

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md ##### Stats

// COMMAND ----------

display(fullDF.summary())

// COMMAND ----------

// MAGIC %md ##### Séléctionner des colonnes

// COMMAND ----------

fullDF.select("PassengerId", "Name", "sex").show(5, false)

// COMMAND ----------

fullDF.select(col("PassengerId"), col("Name"), col("sex")).show(5, false)

// COMMAND ----------

fullDF.select(col("PassengerId").alias("ID"), split(col("Name"), ",").getItem(0).as("firstName"), col("sex").substr(0, 1).as("sex")).show(5, false)

// COMMAND ----------

fullDF.selectExpr("PassengerId as ID", "split(Name,',')[0] as firstName", "substr(sex, 0, 1) as sex").show(5, false)

// COMMAND ----------

// MAGIC %md ##### Renommer une colonne

// COMMAND ----------

val dfRenameCol = fullDF.withColumnRenamed("PassengerId", "ID")
display(dfRenameCol)

// COMMAND ----------

// MAGIC %md ##### Ajouter une nouvelle colonne

// COMMAND ----------

// Ajout Simple

val formatSexCol = fullDF.withColumn("sexFormatted", substring(col("Sex"), 0, 1))
display(formatSexCol)

// COMMAND ----------

// Ajout avec de consitions

val addEstMajeurDF = fullDF.withColumn("estMajeur", when(col("Age").geq(18), 1).otherwise(0))
display(addEstMajeurDF)

// COMMAND ----------

// MAGIC %md ##### Appliquer des filtres

// COMMAND ----------

val lesMajeursDF = addEstMajeurDF.filter(col("estMajeur") <=> 1)
display(lesMajeursDF)

// COMMAND ----------

val lesMajeursDF = addEstMajeurDF.where(col("estMajeur") <=> 1)
display(lesMajeursDF)

// COMMAND ----------

val lesMajeursDF = addEstMajeurDF.where("estMajeur='1'")
display(lesMajeursDF)

// COMMAND ----------

// MAGIC %md ##### Nétoyer les valeurs NULL

// COMMAND ----------

val cleanDF = lesMajeursDF.na.fill("0", Seq("Cabin"))
display(cleanDF)

// COMMAND ----------

val cleanDF = lesMajeursDF.na.drop(Seq("Cabin"))
display(cleanDF)

// COMMAND ----------

val cleanDF = lesMajeursDF.na.replace(Seq("Sex"), Map("female" -> "f", "male" -> "m"))
display(cleanDF)

// COMMAND ----------

// MAGIC %md ##### Aggrégations

// COMMAND ----------

val survivedByPclass = fullDF
                          .groupBy("Pclass")
                          .agg(
                              count("*").as("cnt"), mean("Survived").as("avg")
                          )
                          .orderBy("Pclass")

display(survivedByPclass)

// COMMAND ----------

val survivedBySex = fullDF.groupBy("Sex").agg(count("*").as("cnt"), mean("SurvivedSurvivant").as("avg"))

display(survivedBySex)

// COMMAND ----------

fullDF.groupBy("Pclass").agg(count("*").as("cnt"), mean("Fare").as("Amount"))

// COMMAND ----------

// MAGIC %md ##### Aggrégations avec conditions

// COMMAND ----------

val survivedBySexByPclass = fullDF
                              .groupBy("Sex","Pclass")
                              .agg(
                                  count("*").as("cnt"), mean("Survived").as("avgSurvivant")
                                )
                              .orderBy("Pclass")

display(survivedBySexByPclass)

// COMMAND ----------

val survivedBySexByPclassWithCondition = fullDF
 .groupBy("Sex","Pclass")
 .agg(
 count("*").as("cnt"), 
 count(when(col("Survived") <=> 1, col("PassengerId"))).as("cntSurvived"),              mean("Survived").as("avgSurvivant")
     
     ).orderBy("Pclass")
display(survivedBySexByPclassWithCondition)

// COMMAND ----------

// MAGIC %md ##### Les UDF (User Defined Functions)
// MAGIC 
// MAGIC > val udfFunctionName = udf(scalaFunction)
// MAGIC 
// MAGIC > val udfFunctionName = spark.udf.register("udfFunctionName", scalaFunction)
// MAGIC 
// MAGIC Warning: Null Values

// COMMAND ----------

import scala.util.Random

// Créons un UDF qui renvoie une valeur aléatoire d'une Liste
def chooseRandElement = (A: Array[String]) => A.apply(Random.nextInt(A.size))

val randomFill = udf(chooseRandElement)
val randomFill2 = spark.udf.register("udf_randChoice", chooseRandElement)

// COMMAND ----------

val sampleDF = fullDF.select("PassengerId").sample(0.3)
val nationalite = Array("Afghan","Allemand","Argentin","Autrichien","russe","Bouthan","Britannique","Américain","Sénégalais","Français")

// COMMAND ----------

val newDF = sampleDF.withColumn("Country", randomFill(lit(nationalite)))
display(newDF)

// COMMAND ----------

val newDF2 = sampleDF.withColumn("Country", randomFill2(lit(nationalite)))
display(newDF2)

// COMMAND ----------

// MAGIC %md ##### Appliquer des jointures
// MAGIC 
// MAGIC <img src="/files/join.png" style="float: left;" alt="drawing" width="500"/>

// COMMAND ----------

val joinedDF = fullDF.join(newDF, Seq("PassengerId"), "left")
display(joinedDF)
display(fullDF)

// COMMAND ----------

val joinedDF = fullDF.join(newDF, newDF("PassengerId") <=> fullDF("PassengerId"), "left")

// COMMAND ----------

val left = Seq((1, "A1"), (2, "A2"), (3, "A3"), (4, "A4")).toDF("id", "value")
val right = Seq((3, "A3"), (4, "A4"), (4, "A4_0"), (5, "A5"), (6, "A6")).toDF("id", "value")
val typeJointures = Seq("inner", "full", "right","left","semi", "anti")

println("LEFT")
left.show
println("RIGHT")
right.show

typeJointures.foreach(jointure => {
  println(jointure.toUpperCase + " JOIN")
  left.join(right, Seq("id"), jointure).orderBy("id").show()
})

// COMMAND ----------

left.join(right, Seq("id"), "le")

// COMMAND ----------

// MAGIC %md #### Spark Pour les spécialistes SQL

// COMMAND ----------

fullDF.createOrReplaceTempView("fullDF")

// COMMAND ----------

val aggDF = spark.sql("SELECT Sex, Pclass, count(*) as cnt, mean(Survived) as avgSurvivant FROM fullDF GROUP BY Sex, Pclass ORDER BY Pclass")

display(aggDF)

// COMMAND ----------

// MAGIC %md #### Ecrire une table sous format CSV

// COMMAND ----------

aggDF.write.option("header",true).("delimiter",";").csv("/FileStore/tables/test")

// COMMAND ----------


