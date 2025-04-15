
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import com.github.tototoshi.csv._


object Practica {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .appName("Pratica_Scala")
    .master("local[*]")
    .getOrCreate()


  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  def ejercicio1(): Unit = {

    import spark.implicits._

    val estudiantes = Seq(
      ("Javi", 27, 8),
      ("Alberto", 32, 7),
      ("Pedro", 24, 5),
      ("Raquel", 29, 9),
    ).toDF("nombre", "edad", "calificacion")

    println("Esquema del DataFrame:")
    estudiantes.show()


    val estudiantesAltaCalificacion = estudiantes.filter($"calificacion" > 8)

    println("Estudiantes con calificación mayor a 8:")
    estudiantesAltaCalificacion.show()


    val estudiantesOrdenados = estudiantes.select("nombre", "calificacion")
      .orderBy($"calificacion".desc)

    println("Nombres de estudiantes ordenados por calificación (descendente):")
    estudiantesOrdenados.show()
  }

  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  def ejercicio2(): Unit = {

    import spark.implicits._

    val numeros = (1 to 12).toDF("numero")

    val esParOImpar: UserDefinedFunction = udf((numero: Int) => {
      if (numero % 2 == 0) "par" else "impar"
    })

    val resultado = numeros.withColumn("paridad", esParOImpar(col("numero")))

    resultado.show()
  }

  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  def ejercicio3(): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val estudiantes = Seq(
      (1, "Javi"),
      (2, "Alberto"),
      (3, "Pedro"),
      (4, "Raquel"),
      (5, "Alex")
    ).toDF("id", "nombre")


    estudiantes.show()


    val calificaciones = Seq(
      (1, "Redes", 8),
      (1, "Programación", 8),
      (2, "Redes", 6),
      (2, "Programación", 7),
      (3, "Redes", 9),
      (3, "Programación", 9),
      (4, "Redes", 9),
      (4, "Programación", 7),
      (5, "Redes", 10),
      (5, "Programación", 9),
    ).toDF("id_estudiante", "asignatura", "calificacion")


    calificaciones.show()


    val estudiantesConCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .drop("id_estudiante")


    val promedioAsignaturas = estudiantesConCalificaciones
      .groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio_calificacion"))

    promedioAsignaturas
  }

  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

   */

  def ejercicio4(): Unit = {

    val sc = spark.sparkContext


    val palabras = List(
      "pelicula", "actor", "drama", "comedia", "cine", "director", "escena",
      "guion", "cámara", "acción", "romance", "thriller", "musical", "suspenso",
      "cine", "pelicula", "guion", "estreno", "actor", "banda sonora", "critica",
      "ficcion", "aventura", "cine", "personaje", "villano", "protagonista",
      "cine", "guion", "comedia", "pelicula", "documental", "animacion", "director",
      "pelicula", "drama", "cámara", "director", "actor", "festival", "nominacion"
    )

    val palabrasRDD = sc.parallelize(palabras)

    import spark.implicits._
    val df = palabrasRDD.toDF("palabra")

    val conteoPalabrasDF = df.groupBy("palabra")
      .agg(functions.count("palabra").alias("conteo"))
      .orderBy(functions.col("conteo").desc)

    conteoPalabrasDF.show()



  }

  /**
   Ejercicio 5: Procesamiento de archivos
   Pregunta: Carga un archivo CSV que contenga información sobre
   ventas (id_venta, id_producto, cantidad, precio_unitario)
   y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */

  def ejercicio5(): Unit = {

    import scala.collection.mutable

    case class Venta(id_venta: Int, id_producto: Int, cantidad: Int, precio_unitario: Double)


    val reader = CSVReader.open(new java.io.File("C:\\Users\\javil\\PracticaBD-processing\\src\\main\\resources\\ventas.csv"))

    val ventas = reader.allWithHeaders().map { row =>
      Venta(
        id_venta = row("id_venta").toInt,
        id_producto = row("id_producto").toInt,
        cantidad = row("cantidad").toInt,
        precio_unitario = row("precio_unitario").toDouble
      )
    }


    val ingresosPorProducto = mutable.Map[Int, Double]()


    ventas.foreach { venta =>
      val ingresoTotal = venta.cantidad * venta.precio_unitario
      ingresosPorProducto(venta.id_producto) = ingresosPorProducto.getOrElse(venta.id_producto, 0.0) + ingresoTotal
    }


    ingresosPorProducto.foreach { case (idProducto, ingresoTotal) =>
      println(s"Producto ID: $idProducto - Ingreso Total: $ingresoTotal")
    }


    reader.close()
  }



}
