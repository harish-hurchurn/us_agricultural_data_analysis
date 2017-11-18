package co.ioctl.us_agricultural_data_processor

import scala.collection.mutable.ArrayBuffer

/**
  * This class provides a set of methods which will allow for the download of US Aggricultural data 
  * and will produce a new CSV file with the data sanitized ready for consumption by other tools
  *
  * @param destinationPath The destination where the data is to be stored
  */
case class UsAggricultureDataFileUtils(destinationPath: String) {
  /**
    * A utility to convert a files contents to UTF8
    * 
    * @param originalFile The name and path of the original file which is to be converted
    * @param newfile The name and path where the newly converted file is to be stored
    */
  def convertFileToUtf8(originalFile: String, newfile: String): Unit = {
    try {
      import java.io.{FileInputStream, FileOutputStream}
      val fis = new FileInputStream(originalFile)
      val contents = new Array[Byte](fis.available)
      fis.read(contents, 0, contents.length)
      val asString = new String(contents, "ISO8859_1")
      val newBytes = asString.getBytes("UTF8")
      val fos = new FileOutputStream(newfile)
      fos.write(newBytes)
      fos.close()
    } catch {
      case e: Exception ⇒
        e.printStackTrace()
    }
  }

  /**
    * Will download the US Agriculture data.
    */
  def downloadData(): Unit = {
    /**
      * Will unzip a zip archive 
      *
      * @param zipFile    The name of the zip file which is to be uncompressed
      * @param outputPath The path where the uncompressed content has to be stored
      */
    def unzipFile(zipFile: String, outputPath: String): Unit = {
      import java.io.{FileInputStream, FileOutputStream}
      import java.util.zip.ZipInputStream

      val fis = new FileInputStream(s"$zipFile")
      val zis = new ZipInputStream(fis)

      Stream
        .continually(zis.getNextEntry)
        .takeWhile(_ != null)
        .foreach { file =>
          val fout = new FileOutputStream(s"$outputPath/${file.getName}")
          val buffer = new Array[Byte](1024)


          Stream.continually(zis.read(buffer))
            .takeWhile(_ != -1)
            .foreach(fout.write(buffer, 0, _))
        }
    }

    import java.io.{File ⇒ JFile}
    import java.net.URL

    import scala.language.postfixOps
    import sys.process._

    new URL("http://usda.mannlib.cornell.edu/usda/current/CropProg/CropProg-11-13-2017.zip") #> new JFile(s"$destinationPath/data.zip") !!

    unzipFile(s"$destinationPath/data.zip", s"$destinationPath")
  }

  
  /**
    * This method will go through the downloaded CSV file and remove elements of the file which is 
    * pertinent 
    * 
    * @return A List of CSV lines which we are interested in
    */
  def sanitizeCsvFile(originalFile: String, newFile: String): Unit = {
    
    /**
      * Will read a CSV file from the path specified 
      *
      * @param path The path where the file can be found
      * @return A List of array where each element of the array is a line of CSV
      */
    def readCsvFile(path: String): List[Array[String]] = {
      def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
        try {
          f(resource)
        } finally {
          import scala.language.reflectiveCalls
          resource.close()
        }

      val rows: ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()

      using(scala.io.Source.fromFile(path)) {
        source =>
          for (line <- source.getLines) {
            rows += line.split(",").map(_.trim)
          }
      }

      rows.toList
    }
    
    /**
      * Writes the contents to a file on disk 
      *
      * @param filename the name of the file to be written
      * @param data the data to be written
      */
    def writeCsvFile(filename: String, data: List[Array[String]]): Unit = {
      import java.io.{BufferedWriter, Closeable, FileOutputStream, OutputStreamWriter}

      def using[T <: Closeable, R](resource: T)(f: T => R): R = {
        try {
          f(resource)
        } finally {
          resource.close()
        }
      }

      using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(s"$destinationPath/$filename")))) {
        writer =>
          for (x <- data) {
            writer.write(x.mkString("", ",", "\n"))
          }
      }
    }
    
    var array = ArrayBuffer[Array[String]]()
    
    convertFileToUtf8(s"$destinationPath/$originalFile", s"$destinationPath/$newFile")

    readCsvFile(s"$destinationPath/$newFile")
      .zipWithIndex.foreach { 
      case (a: Array[String], _) ⇒
        a(1) match {
          case "\"t\"" ⇒ // Ignore titles
          case "\"h\"" ⇒ array += a
          case "\"u\"" ⇒ array += a
          case "\"d\"" ⇒ array += a
          case "\"c\"" ⇒ // Ignore end of file
        }
    }
    
    writeCsvFile(newFile, array.toList)
  }
}

object CsvPreprocessing extends App {
  val usAggricultureDataFileUtils = UsAggricultureDataFileUtils(destinationPath = args(0))
  
  // Download the US agriculture data file
  usAggricultureDataFileUtils.downloadData()
  
  // Load the original file and remove any entries which we are not interested in and write the new file to disk
  usAggricultureDataFileUtils.sanitizeCsvFile("prog_p01b_t011.csv", "sanatized_prog_p01b_t011.csv")
}
