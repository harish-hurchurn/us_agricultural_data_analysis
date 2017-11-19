name := "US_Crop_Data_Analysis"

lazy val usAgricultureAnalysis = (project in file("."))
  .settings(UsAgricultureDataAnalysis.settings: _*)
  .aggregate(csvPreprocessing, soybeanProcessing)

lazy val csvPreprocessing = (project in file("csv_processing"))
  .settings(UsAgricultureDataAnalysis.settings: _*)
  .settings(
    name := "csv_preprocessing",
    libraryDependencies ++= UsAgricultureDataAnalysis.Dependencies.dependencies,
    version := "0.1.0"
  )

lazy val soybeanProcessing = (project in file("soybean_processing"))
  .settings(UsAgricultureDataAnalysis.settings: _*)
  .settings(
    name := "soybean_processing",
    libraryDependencies ++= UsAgricultureDataAnalysis.Dependencies.dependencies,
    version := "0.1.0"
  )


