// Databricks notebook source
// MAGIC %md # Scala Basics

// COMMAND ----------

// MAGIC %md ## Syntaxes de bases

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Le caractère ‘;’ en fin d’instruction est optionnel sauf si plusieurs instructions sur une ligne 
// MAGIC - Sensible à la casse
// MAGIC - Le nom des classes débute par une majuscule
// MAGIC - Les methodes commencent par un minuscule
// MAGIC - Le nom du fichier du programme doit correspondre au nom de l’objet en question
// MAGIC - La fonction def main(args: Array[String]) est obligatoire et représente le point d’entré du programme
// MAGIC - Les noms de variables, d’objets, de classes, de fonctions débutent par une lettre ou un underscore. Ex: nom, _nom, _1_nom …
// MAGIC - Commentaires: 
// MAGIC     - //          => une ligne
// MAGIC     - /* … */  => multiple lignes
// MAGIC - Import de modules: 
// MAGIC     - import org.me._ 			=> importe toutes les méthodes dans me
// MAGIC     - import org.me.porterHabit 		=> n’importe que la méthode porterHabit
// MAGIC     - import org.me.{seLaver, porterHabit} 	=> n’importe que les méthodes porterHabit et seLaver

// COMMAND ----------

// MAGIC %md ### Déclaration des variables

// COMMAND ----------

val nom: String = "FALL"

// COMMAND ----------

nom = "NIANG"

// COMMAND ----------

var prenom = "Ibrahima"

// COMMAND ----------

prenom = "Khalil"

// COMMAND ----------

prenom

// COMMAND ----------

val profession = "Computer Scientist"

// COMMAND ----------

val full_ex = 15

// COMMAND ----------

lazy val full_ex = { println("y"); 15 }

// COMMAND ----------

full_ex

// COMMAND ----------

full_ex

// COMMAND ----------

val (x, y, z) = ("FALL","Ibrahima", 25)

// COMMAND ----------

println(x)

// COMMAND ----------

println("My name is " + prenom + " and I am a " + profession)

// COMMAND ----------

println(s"My name is $prenom and I am a $profession")

// COMMAND ----------

// Tester 2 string, null-safe
val fruit1 = "Mangue"
val fruit2 = "Mangue"
val fruit3 = null

val test1  = fruit1 == fruit2
val test2  = fruit1 == fruit3

// COMMAND ----------

// MAGIC %md ### Data Types

// COMMAND ----------

val num: Int                = 1       
val bool: Boolean           = true
val bool2                   = 1 > 2
val letter: Char            = 'a'
val pi: Double              = 3.14
val bigNumber: Long         = 123456789
val NombreAssezGrand: Short = 3267  
val smallNumber: Byte       = 123     

// COMMAND ----------

println(f"My name is $prenom and I am a $profession with $num year(s) experience.")

// COMMAND ----------

println(s"We can put whatever we want in the brackets like ${smallNumber*pi}.")

// COMMAND ----------

//Opérations booléenne
val boolType = true
val negBool  = !boolType
val andBool  = (4 > 3) & (false) //false and false => false
val orBool   = (4 > 3) | (true)  //true or false => true

// COMMAND ----------

// MAGIC %md ### Manipulations des Strings

// COMMAND ----------

val lePlusLongMot: String = "anticonstitutionnellement"

// COMMAND ----------

//Fonctions de bases
println(lePlusLongMot)
val laPremiereLettre      = lePlusLongMot(0)
val laPremiereLettreEnMaj = lePlusLongMot.capitalize
val motEnMaj              = lePlusLongMot.toUpperCase
val suffixe               = lePlusLongMot.slice(0,4)
val mirroirMot            = lePlusLongMot.reverse
val is_e_in_word          = lePlusLongMot.contains('e')
val nunmOccurOfE          = lePlusLongMot.count(x => x == 'e')
val stringToList          = lePlusLongMot.toList
val concatMot             = lePlusLongMot.concat("-Ment")
val dropSuffixe           = lePlusLongMot.replace(suffixe, "")

// COMMAND ----------

//Split une chaine
val date: String = "2020-01-01"
date.split("-")

// COMMAND ----------

//Changer le contenu d'un String
lePlusLongMot(0) = 'u'

// COMMAND ----------

// MAGIC %md ## Collections

// COMMAND ----------

// MAGIC %md ### Lists

// COMMAND ----------

val fruits: List[String] = List("pomme", "banane", "goyave", "banane", "kaki", "pomplemousse")

// COMMAND ----------

//Accéder à un élémént d'une Liste
val deuxiemeElement  = fruits.apply(1)
val troisiemeElement = fruits(2)

// COMMAND ----------

//Récupérer une sous-liste
val sousliste1 = fruits.slice(2, 4)
val sousliste2 = fruits.takeRight(3)
val sousliste3 = fruits.take(2)
val sousliste4 = fruits.takeWhile(x => x.length >= 5)
val sousListe5 = fruits.filter(x => x.length > 6)

// COMMAND ----------

//Ajouter un élément dans une Liste (en mode append et prepend)
println(fruits)
val prependListe = "ananas" +: fruits
val appendListe  = fruits :+ "pastèque"

// COMMAND ----------

//Existence d'un element dans la liste
fruits.contains("goyave") 

// COMMAND ----------

//Taille d'unne liste
fruits.length

// COMMAND ----------

//Compter le nombre d'éléments dans une liste
fruits.count(x => x == "banane") 

// COMMAND ----------

//Inversé une Liste
fruits.reverse

// COMMAND ----------

//Merge 2 Lists
val fruitsAcide = List("Citron", "Orange","Mandarine")
val concatList1 = fruits ::: fruitsAcide
val concatList2 = fruits ++: fruitsAcide

// COMMAND ----------

//Ordonner une Liste
println(fruits)
val alphabetSorting  = fruits.sorted
val numLetterSorting = fruits.sortBy(_.length)
val customSorting    = fruits.sortBy(_.apply(3))
val consitionSorting = fruits.sortWith((x, y) => x.contains('k'))

// COMMAND ----------

//Supprimer des éléments
println(fruits)
val dropPremierElement       = fruits.drop(1)
val drop2DerniersElements    = fruits.dropRight(2)
val dropElementAvecCondition = fruits.dropWhile(x => x.length > 6)
val dropBigFruits            = fruits.filter(x => !(x.length > 6))
val isListEmpty              = dropElementAvecCondition.isEmpty

// COMMAND ----------

//Changer les valeurs d'une liste
fruits(1) = "Banane"

// COMMAND ----------

// MAGIC %md ### Les Arrays

// COMMAND ----------

val ages: Array[Int] = Array(15, 18, 56, 34, 78, 23, 28)
val members: Array[String] = Array("Modou", "Tapha", "Vieux", "")

// COMMAND ----------

//Changer les valeurs d'un Array
ages

// COMMAND ----------

ages(0) = 90

// COMMAND ----------

ages

// COMMAND ----------

// MAGIC %md ### Le type Option

// COMMAND ----------

val nomOption: Option[String]    = Some("FALL")
val prenomOption: Option[String] = None

// COMMAND ----------

//Récupérer la valeur
val nom1   = nomOption.getOrElse("nom not found !")
val nom2   = nomOption.get
val prenom1 = prenomOption.getOrElse("Name not found !")

// COMMAND ----------

//Tester les variables Option
val estDefinie1 = nomOption.isDefined
val estDefinie2 = prenomOption.nonEmpty
val estDéfinie3 = nomOption.isEmpty

// COMMAND ----------

Some(null)

// COMMAND ----------

division(1, 0)

// COMMAND ----------

import scala.util.{Try,Success,Failure}
def divideXByY(x: Int, y: Int): Try[Int] = {
    Try(x / y)
}

// COMMAND ----------

// MAGIC %md ### Les Set

// COMMAND ----------

val maSet1: Set[Int] = Set(1,4,7,1,55,24)
val maSet2: Set[Int] = Set(2,7,9,1,33)

// COMMAND ----------

//Tester l'existence d'un élément
val existence1 = maSet1.contains(7)
val existence2 = maSet1(7)
val existence3 = maSet1.apply(7)

// COMMAND ----------

//Opérations sur les ensembles
val intersections = maSet1.intersect(maSet2)
val difference1    = maSet1.diff(maSet2)
val difference2    = maSet2.diff(maSet1)
val difference3    = maSet1.diff(maSet2)
val sousEnsembles  = maSet2.subsets
val isSubsetOfSet1 = Set(1,4).subsetOf(maSet1)
val isSubsetOfSet2 = Set(1,4).subsetOf(maSet2)

// COMMAND ----------

sousEnsembles.foreach(println)

// COMMAND ----------

// MAGIC %md ### Les tuples

// COMMAND ----------

val maTup : Tuple3[String,String,Int]=Tuple3("dia", "mor", 28)

// COMMAND ----------

val maTup : Tuple3[String,String,Int]=Tuple3("dia", "mor", 28, "yeumbeul")

// COMMAND ----------

//Accéder aux éléments d'un tuple
val premierElement   = maTup._1
val deuxiemeElement  = maTup._2
val troisiemeElement = maTup._3
val autreOption      = maTup.productElement(1)

// COMMAND ----------

//La taille d'un tuple
val taille = maTup.productArity

// COMMAND ----------

// MAGIC %md ### Les Map

// COMMAND ----------

val maMap: Map[String, String] = Map("nom"-> "dia", "prenom"->"mor", "age"->"28")

// COMMAND ----------

// Accéder aux éléments
val clefs      = maMap.keys
val valeurs    = maMap.values
val getNom     = maMap("nom")
val getPrenom  = maMap.get("nom")
val getSex     = maMap.get("sex")
val getSex1    = maMap.getOrElse("sex", "Key unknown !")

// COMMAND ----------

//Conversion
val mapToList = maMap.toList
val mapToArray = maMap.toArray

// COMMAND ----------

// MAGIC %md ## Conditionnel et Boocles

// COMMAND ----------

// MAGIC %md ### If - Else

// COMMAND ----------

//import scala.io.StdIn._
//val age = readInt()
val age = 23
if( age>=18) {
	println("Vous êtes majeur(e) !")
}
else {
	println("Vous n’êtes pas encore majeur(e) !")
}

// COMMAND ----------

val sex = "M"
val simpleIfElse = if(sex.equalsIgnoreCase("m")) "Homme" else "Femme"

// COMMAND ----------

// MAGIC %md ### For Loop

// COMMAND ----------

for( i <- 1 to 100 by 2) {
    print("*")
	print(i)
    print("*")
}

// COMMAND ----------

for( i <- 1 to 100 if(i%2== 0); if(i.toString.contains('7'))) {
    print("*")
	print(i)
    print("*")
}

// COMMAND ----------

val nombreMagique = for( i <- 1 to 100 if(i%2== 0); if(i.toString.contains('7'))) yield i

// COMMAND ----------

// MAGIC %md ### While Loop

// COMMAND ----------

var i = 1
while( i <= 100) {
	print("*")
	print(i)
    print("*")
	i += 2
}


// COMMAND ----------

// MAGIC %md ### Do While Loop

// COMMAND ----------

var i = 1
do{
	print("*")
	print(i)
    print("*")
	i += 2
} while( i <= 100) 


// COMMAND ----------

// MAGIC %md ### Pattern Matching

// COMMAND ----------

var age = 18
age match {
	case 18	 	        => println("Vous avez 18 ans.")
	case x if (x < 18)	=> println("Vous avez moins de 18 ans.")
	case 19 | 20	    => println("Vous avez 19 ou 20 ans.")
	case _ 	 	        => println("Vous avais plus de 20 ans")
}

// COMMAND ----------

var age: Any  = 18
age match {
	case x: Int	    => println("Un Int")
	case x: String  => println("Un String")
    case _          => println("Unknown.")
}

// COMMAND ----------

// MAGIC %md ### Les fonctions

// COMMAND ----------

// MAGIC %md #### Syntaxe de création de fonctions
// MAGIC 
// MAGIC >**def** *nomFunction*( [args: *typeArgs*] ) [: *typeOutput* ] = {
// MAGIC 
// MAGIC >      ... body ...
// MAGIC 
// MAGIC >      [return output]
// MAGIC >}
// MAGIC 
// MAGIC Les éléments entre [ ] sont optionnels.

// COMMAND ----------

def sumInts(x: Int, y: Int): Int = {
  val sum = x + y
  return sum
}

sumInts(3,8)

// COMMAND ----------

def sumInts(x: Int, y: Int) = {
  x + y
}

sumInts(3,8)

// COMMAND ----------

// MAGIC %md ### Fonctions Lambda
// MAGIC 
// MAGIC ##### Syntaxe
// MAGIC > ( args: *typeArgs* ) => ... body ...
// MAGIC 
// MAGIC Les éléments entre [ ] sont optionnels.

// COMMAND ----------

//Appelle d'une fonction anonyme
((x: Int,y: Int) => x + y)(1, 2)

// COMMAND ----------

//Donner un nom à la fonciton anonyme
val additionner = (x: Int,y: Int) => x + y

// COMMAND ----------

additionner(1,3)
