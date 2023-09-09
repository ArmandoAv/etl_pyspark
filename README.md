# Proyect etl_pyspark

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li>
          <a href="#installation">Installation</a>
          <ul>
            <li><a href="#python">Python</a></li>
            <li><a href="#java">Java</a></li>
            <li><a href="#apache-spark">Apache Spark</a></li>
            <li><a href="#winutils">Winutils</a></li>
            <li><a href="#sql-server">SQL Server</a></li>
            <li><a href="#env-file">env file</a></li>
          </ul>
        </li>
      </ul>
    </li>
    <li>
      <a href="#usage">Usage</a>
      <ul>
        <li><a href="#extract">Extract</a></li>
        <li><a href="#transform">Transform</a></li>
        <li><a href="#load">Load</a></li>
        <li><a href="#considerations">Considerations</a></li>
        <li><a href="#validation">Validation</a></li>
      </ul>
    </li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->

## About The Project

This project contains the entire track of an ETL process on Windows. Several transformations are performed from a json input file until information is loaded into several tables of an entity relationship model in SQL Server.

### Built With

The following software was used to carry out this project:

- Python
- Apache Spark
- Java
- SQL Server

<!-- GETTING STARTED -->

## Getting Started

This is an example of how you may give instructions on setting up your project locally. To get a local copy up and running follow these simple example steps.

### Prerequisites

The following list is what is needed to use the software.

- Python 3.7.3
- Apache Spark 2.4.3
- Jdk of Java 1.8_u60 or higher
- Jre of Java 1.8_u60 or higher
- winutils.exe
- SQLEXPRADV_x64_ENU.exe
- apache-spark-sql-connector.jar

In the following link is the version of Python 3.7.3

https://www.python.org/downloads/release/python-373/

In the following link there is the version of Apache Spark 2.4.3

https://archive.apache.org/dist/spark/

In the following link there are different versions of Java 8

https://www.oracle.com/mx/java/technologies/javase/javase8-archive-downloads.html

In the following link there is the express version of SQL Server

https://www.microsoft.com/es-mx/sql-server/sql-server-downloads

In the software path are the files

- winutils.exe
- apache-spark-sql-connector.jar

### Installation

To install the software mentioned above, the following steps must be performed. The following steps are to install the required software in a windows environment.

#### Python

To install Python, run the file python-3.7.3-amd64.exe which was downloaded from the link. When executing the file, in the same process it indicates that if you want to save the Python variable in the environment variables, so it is no longer necessary to add them when finishing the installation.

A cmd terminal is opened and validated, execute the next command

```
python --version
```

Python has a library with which you can install plugins on the command line called pip. Copy the script from the following link into a notepad

https://bootstrap.pypa.io/get-pip.py

Save the copied script as get-pip.py and open a cmd terminal in the path where you saved the file and executed the following command

```
python get-pip.py
```

The following environment variable must be created

```
PIP_HOME = C:\Users\.....\AppData\Local\Programs\Python\Python37
```

The %PIP_HOME%\Scripts variable is added to the Path variable.

#### Java

To install jdk and jre, run the file jdk-...-i586.exe which was downloaded from the link. When executing the file, the paths C:\jdk and C:\jre must be indicated when installing each one. When the jdk and jre are installed, the following environment variable must be created

```
JAVA_HOME = C:\jdk
```

The %JAVA_HOME%\bin variable is added to the Path variable.

A cmd terminal is opened and it is validated that any of the following commands are valid.

```
java
java --version
```

#### Apache Spark

Once you have the spark-2.4.3-bin-hadoop2.7.tgz file, you must create the C:\spark directory and copy all the contents of the file to that path.

Once the content has been copied, look in the C:\spark\conf directory for the log4j.properties.template file and rename it as log4j.properties. Then open the renamed file with a text editor and on the next line of the file

```
log4j.rootCategory=INFO, console
```

It should be changed to

```
log4j.rootCategory=ERROR, console
```

This is done so that when executing Spark commands in a terminal, it does not fill up with information that is not necessary. The following environment variable must be created

```
SPARK_HOME = C:\spark
```

The %SPARK_HOME%\bin variable is added to the Path variable.

Finally, the apache-spark-sql-connector.jar file must be copied to the path C:/spark/jars in order to make connections to the SQL Server.

#### Winutils

You must create the C:\winutils\bin directory and copy the winutils.exe file to that path. Once the file has been copied, the following environment variable must be created

```
HADOOP_HOME = C:\winutils
```

The %HADOOP_HOME%\bin variable is added to the Path variable.

A cmd terminal is opened and validated, execute the nexts commands, if everything works correctly, the Spark environment will start.

```
cd C:\spark
pyspark
```

#### SQL Server

To install SQL Server, run the file SQLEXPRADV_x64_ENU.exe which was downloaded from the link, creating the 'sa' user that is the administrator of the server and which has all the permissions of the database.

Once the SQL Server is installed, you must open the SQL Server Management Studio and copy the contents of the Script_Database_Model.sql file located in the database directory.

The content must be executed in parts

The CREATE DATABASE script
The CREATE TABLES script
The CREATE VIEWS script

The other scripts in the file are to delete the tables and restart the sequences created, to be able to perform various load tests.

#### env file

It is necessary to create an environment file called .env in the path where the project will be. The file must contain the following

```
# File with connector parameters
CONN = com.microsoft.sqlserver.jdbc.spark
SQL_USER = your_db_user
SQL_PWD = your_db_pwd
SQL_DB = UBER_ANALISIS
SQL_SERVER = localhost
```

This is an environment variables file and isn't a Python file, so variables aren't enclosed in quotation marks.

<!-- USAGE EXAMPLES -->

## Usage

To run this project, you first need to make a copy of the files and directories, this can be done with the following command in a cmd terminal

```
git clone https://github.com/ArmandoAv/etl_pyspark.git
```

Once you have all the files locally, you can run the process.
The process is divided into three parts

1. Extraction
1. Transformation
1. Load

Since it is an ETL process, it must be executed in order. A Python virtual environment is needed, the following command must be executed from a cmd terminal in the path where the project was copied.

```
python -m venv venv
```

Once the virtual environment is created, it is activated in the same terminal with the following command

```
venv\Scripts\activate
```

To deactivate the virtual environment in the same path you have to execute the following command

```
deactivate
```

The following python libraries should be installed with the following commands

```
pip install python-dotenv
pip install python-decouple
```

The files that must be executed are the ones in the src path, the files that are in the aux_src path contain variables or functions that help in the execution of the files.

> [!NOTE]
> The execution of the processes must be carried out on the same day, since the files that are generated in their names have the current date, so if they are executed on a different day, the **transform** and **load** processes will fail because they cannot find the files.

### Extract

To execute the extract process, you only have to execute the Extract.py file with the following command in the src path

```
spark-submit Extract.py
```

This process creates a log file in the logs path automatically, but if it is also required to obtain an out type file with the information that the console leaves, you must execute it with the following command, the file will be generated in the logs directory

```
spark-submit Extract.py | tee ..\logs\Extract_Process_First_Execution.out
```

This process reads the json file from the input path and creates a csv file in the output path, which will be used to create the corresponding files to be able to load the tables created in the SQL Server.

### Transform

To execute the transform process, you only have to execute the Transform.py file with the following command in the src path

```
spark-submit Transform.py
```

This process creates a log file in the logs path automatically, but if it is also required to obtain an out type file with the information that the console leaves, you must execute it with the following command, the file will be generated in the logs directory

```
spark-submit Transform.py | tee ..\logs\Transform_Process_First_Execution.out
```

This process creates seven files from the csv file created in the previous process in the output path, these files are the basis for loading the information in the tables created in the SQL Server.

### Load

To execute the load process, you only have to execute the Load.py file with the following command in the src path

```
spark-submit Load.py
```

This process creates a log file in the logs path automatically, but if it is also required to obtain an out type file with the information that the console leaves, you must execute it with the following command, the file will be generated in the logs directory

```
spark-submit Load.py | tee ..\logs\Load_Process_First_Execution.out
```

This process takes the files created in the previous process and performs loads in the image and temporary tables. After performing these first loads, create new files comparing the information from the final tables of the model to load only the new information.

This functionality was made to make the uploads more efficient and simulate the uploads to a DWH. With the new files created, the load is done towards the final tables.

Finally, the process deletes the created files and creates a copy of the json file and the first csv file in the process path.

### Considerations

Add info...

### Validation

At the end of executing the loading process, it is validated that the information is already available in the database with the SELECT VIEWS script, from the Script_Database_Model.sql file of the database route.

To see more clearly how the created validations work, for information management. The process must be executed once more, since the process not finding new information, will not carry out the loads to the final tables, you must execute them with the following commands

```
spark-submit Extract.py | tee ..\logs\Extract_Process_Second_Execution.out
spark-submit Transform.py | tee ..\logs\Transform_Process_Second_Execution.out
spark-submit Load.py | tee ..\logs\Load_Process_Second_Execution.out
```

<!-- CONTRIBUTING -->

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
1. Create your feature branch (`git checkout -b feature/AmazingFeature`)
1. Adding your changes in the staging area (`git add -A`)
1. Commit your changes with your comments (`git commit -m 'Add some AmazingFeature'`)
1. Push to the branch (`git push origin feature/AmazingFeature`)
1. Open a Pull Request

<!-- LICENSE -->

## License

This project can be used to see how an ETL process works with Spark. The dataset used for this project is based on the file uber_data.csv in the https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project project.

For more info about dataset can be found here:

1. Website - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
1. Data Dictionary - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

<!-- CONTACT -->

## Contact

You can contact me in my LinkedIn profile

Armando Avila - [@Armando Avila](https://www.linkedin.com/in/armando-avila-419a8623/)

Project Link: [https://github.com/ArmandoAv/etl_pyspark](https://github.com/ArmandoAv/etl_pyspark)
