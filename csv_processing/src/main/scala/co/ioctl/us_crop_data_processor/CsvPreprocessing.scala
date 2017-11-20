package co.ioctl.us_crop_data_processor

/**
  * This class provides a set of methods which will allow for the download of US Aggricultural data 
  * and will produce a new CSV file with the data sanitized ready for consumption by other tools
  *
  * @param destinationPath The destination where the data is to be stored
  */
case class UsAggricultureDataFileUtils(destinationPath: String) {
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
    import scala.collection.mutable.ArrayBuffer
    /**
      * A utility to convert a files contents to UTF8
      *
      * @param originalFile The name and path of the original file which is to be converted
      * @param newfile      The name and path where the newly converted file is to be stored
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
      * Will read a CSV file from the path specified 
      *
      * @param path The path where the file can be found
      * @return A List of csvLine where each element of the csvLine is a line of CSV
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
      * @param data     the data to be written
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

    convertFileToUtf8(s"$destinationPath/$originalFile", s"$destinationPath/$newFile")

    /**
      * Process the headers
      * 
      * @param csvLine
      * @return
      */
    def processHeader(csvLine: Array[String], header: ArrayBuffer[Array[String]]): ArrayBuffer[Array[String]] = {
      val stringifiedArray = csvLine.mkString("", ",", "") // The line which is being processed but split by commas
      
      // Process the columns which have words Week ending
      if (stringifiedArray.contains("Week ending")) {
        header += Array("Week ending")
      }

      // Process the columns which the word state
      if (stringifiedArray.contains("State"))
        header += Array("State")
      
      // Process the columns which have the word average and create a column called Condition
      if (stringifiedArray.contains("Average"))
        header += Array("Condition")
      
      // Process the columns which have months of the year with the day of the month. Have to do some hackery here in order to put all of the columns which are related to Week ending in one column
      val dayOfMonthRegEx = """(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,}""".r // Regular expression to match the month with the day and comma
      val monthAndDayMatch = dayOfMonthRegEx.findAllIn(stringifiedArray)

      if (monthAndDayMatch.length == 3) {
        header.zipWithIndex.foreach {
          case (weekEndingArray, index) ⇒
            if (weekEndingArray.mkString == "Week ending") {
              
              var arrayBuffer = ArrayBuffer[String](weekEndingArray(0))
              
              dayOfMonthRegEx.findAllIn(stringifiedArray).toList.foreach { line ⇒
                arrayBuffer += line
              }

              header(index) = arrayBuffer.toArray
            }
        }
        
        // Process the lines which have the years columns - for example 2016 or 2017
        val prepared = stringifiedArray.replace("\"", "")
        val yearsRegex = """(\d\d\d\d,\d\d\d\d,\d\d\d\d)""".r
        val yearMatched = yearsRegex.findAllIn(stringifiedArray)

        if (yearsRegex.findAllIn(prepared).length == 1) {

          // Need to find the csvLine holding the week ending
          header.zipWithIndex.foreach {
            case (weekEndingArray: Array[String], index: Int) ⇒
              if (weekEndingArray.mkString.contains("Week ending")) {

                val t = yearsRegex.findAllIn(prepared).toArray.mkString("", " ", "").split(",")

                import java.time.LocalDate
                import java.time.format.DateTimeFormatter
                import java.util.Locale

                val formatter = DateTimeFormatter.ofPattern("MMMM d yyyy", Locale.ENGLISH)

                header(0)(1) = LocalDate.parse(header(0)(1).concat(" " + t(0)), formatter).toString
                header(0)(2) = LocalDate.parse(header(0)(2).concat(" " + t(1)), formatter).toString
                header(0)(3) = LocalDate.parse(header(0)(3).concat(" " + t(2)), formatter).toString
              }
          }
        }
      }
      
      header
    }

    /**
      * Process the units
      * 
      * @param csvLine
      */
    def processUnit(csvLine: Array[String], unitInfo: ArrayBuffer[Array[String]]): ArrayBuffer[Array[String]] = {
      val stringifiedArray = csvLine.mkString("", ",", "") // The line which is being processed but split by commas
      
      if (stringifiedArray.contains("(percent)"))
        unitInfo += Array("Percent")

      unitInfo
    }

    val csvFile = ArrayBuffer[Array[String]]()
    
    readCsvFile(s"$destinationPath/$newFile")
      .zipWithIndex.foreach {
      case (csvLine, _) ⇒
        csvLine(1) match {
          case "\"t\"" ⇒ // Ignore titles
          case "\"h\"" ⇒ processHeader(csvLine, csvFile)
          case "\"u\"" ⇒ processUnit(csvLine, csvFile)
          case "\"d\"" ⇒ 
          case "\"c\"" ⇒ // Ignore end of file
        }
    }

    writeCsvFile(newFile, csvFile.toList)
  }
}

object CsvPreprocessing extends App {
  val usAggricultureDataFileUtils = UsAggricultureDataFileUtils(destinationPath = args(0))

  // Download the US agriculture data file
  usAggricultureDataFileUtils.downloadData()

  // Load the original file and remove any entries which we are not interested in and write the new file to disk
  usAggricultureDataFileUtils.sanitizeCsvFile("prog_p01b_t011.csv", "sanatized_prog_p01b_t011.csv")
}
