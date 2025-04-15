import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.functions._
import utils.TestInit


class PracticaTest extends TestInit {


  import spark.implicits._

  "ejercicio1" should "Primer Ejercicio Ejecutado" in {


    val estudiantes = Seq(
      ("Javi", 27, 8),
      ("Alberto", 32, 7),
      ("Pedro", 24, 5),
      ("Raquel", 29, 9),
    ).toDF("nombre", "edad", "calificacion")


    val estudiantesAltaCalificacion = estudiantes.filter($"calificacion" > 8)


    assert(estudiantesAltaCalificacion.count() == 1)


    val estudiantesOrdenados = estudiantes.select("nombre", "calificacion")
      .orderBy($"calificacion".desc)


    val estudiantesOrdenadosArray = estudiantesOrdenados.collect().map(row => (row.getAs[String]("nombre"), row.getAs[Int]("calificacion")))
    assert(estudiantesOrdenadosArray.sameElements(Array(("Raquel", 9), ("Javi", 8), ("Alberto", 7), ("Pedro", 5))))

    Practica.ejercicio1()
  }

  "ejercicio2" should "Determina si es par o impar" in {


    val numeros = (1 to 12).toDF("numero")

    val esParOImpar: UserDefinedFunction = udf((numero: Int) => {
      if (numero % 2 == 0) "par" else "impar"
    })


    val numerosConParidad = numeros
      .withColumn("paridad", esParOImpar(col("numero")))
      .select("numero", "paridad")
      .orderBy("numero")


    val resultado = numerosConParidad.collect()


    (1 to 12).zipWithIndex.foreach { case (num, idx) =>
      val esperado = if (num % 2 == 0) "par" else "impar"
      assert(resultado(idx).getAs[String]("paridad") == esperado)
    }
    Practica.ejercicio2()
  }


  "ejercicio3" should "calcula el promedio de calificaciones" in {

    import spark.implicits._
    import Practica.ejercicio3

    val resultadosEsperados = Seq(
      (1, "Javi", 8.0),
      (2, "Alberto", 6.5),
      (3, "Pedro", 9.0),
      (4, "Raquel", 8.0),
      (5, "Alex", 9.5)
    ).toDF("id", "nombre", "promedio_calificacion")

    val actual = ejercicio3().orderBy("id").collect()
    val expected = resultadosEsperados.orderBy("id").collect()

    assert(actual.sameElements(expected))
  }

  "ejercicio4" should "cuenta el número de veces que se repite una palabra" in {

    val palabras = List(
      "pelicula", "actor", "drama", "comedia", "cine", "director", "escena",
      "guion", "cámara", "acción", "romance", "thriller", "musical", "suspenso",
      "cine", "pelicula", "guion", "estreno", "actor", "banda sonora", "critica",
      "ficcion", "aventura", "cine", "personaje", "villano", "protagonista",
      "cine", "guion", "comedia", "pelicula", "documental", "animacion", "director",
      "pelicula", "drama", "cámara", "director", "actor", "festival", "nominacion"
    )

    val palabrasRDD = spark.sparkContext.parallelize(palabras)

    val df = palabrasRDD.toDF("palabra")

    val conteoPalabrasDF = df.groupBy("palabra")
      .agg(functions.count("palabra").alias("conteo"))
      .orderBy(functions.col("conteo").desc)

    val palabrasDiferentes = conteoPalabrasDF.count()
    assert(palabrasDiferentes > 0, "Debe haber palabras en el DataFrame")

    val resultados = conteoPalabrasDF.collect()

    resultados.take(5).foreach {
      case Row(palabra: String, conteo: Long) =>
        println(s"Palabra: $palabra, Conteo: $conteo")
    }

    assert(resultados.exists(row => row.getString(0) == "pelicula"), "Debe contener la palabra 'pelicula'")
    assert(resultados.exists(row => row.getString(0) == "cine"), "Debe contener la palabra 'cine'")
    assert(resultados.exists(row => row.getString(0) == "guion"), "Debe contener la palabra 'guion'")
    assert(resultados.exists(row => row.getString(0) == "director"), "Debe contener la palabra 'director'")

    val totalOcurrencias = resultados.map(_.getLong(1)).sum
    assert(totalOcurrencias == palabras.length, s"El número total de ocurrencias debe ser igual a la longitud de la lista: $totalOcurrencias")

  }

  "ejercicio5" should "calcula el ingreso total (cantidad * precio_unitario) por producto" in {

    case class Venta(id_venta: Int, id_producto: Int, cantidad: Int, precio_unitario: Double)

    val ventas = Seq(
      Venta(1, 101, 5, 10.0),
      Venta(2, 102, 2, 20.0),
      Venta(3, 101, 3, 10.0)
    )

    val ingresosPorProducto = ventas
      .groupBy(_.id_producto)
      .map { case (id_producto, ventasProducto) =>
        val total = ventasProducto.map(v => v.cantidad * v.precio_unitario).sum
        (id_producto, total)
      }

    assert(ingresosPorProducto(101) == 80.0, "El ingreso total para el producto 101 es incorrecto")
    assert(ingresosPorProducto(102) == 40.0, "El ingreso total para el producto 102 es incorrecto")
  }
}
