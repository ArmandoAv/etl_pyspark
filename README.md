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
    <li><a href="#usage">Usage</a></li>
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

In the directory software are the files

winutils.exe
apache-spark-sql-connector.jar.

### Installation

To install the software mentioned above, the following steps must be performed.

#### Python

This project is done with previous knowledge of Python, so the software installation steps are not reviewed. The following python libraries should be installed with the following commands

pip install python-dotenv
pip install python-decouple

#### Java

To install jdk and jre, run the file jdk-...-i586.exe which was downloaded from the link. When executing the file, the paths C:\jdk and C:\jre must be indicated when installing each one. When the jdk and jre are installed, the following environment variable is created

JAVA_HOME = C:\jdk

The %JAVA_HOME%\bin variable is added to the Path variable.

A cmd terminal is opened and it is validated that the java or java --version command is valid.

#### Apache Spark

Once you have the spark-2.4.3-bin-hadoop2.7.tgz file, you must create the C:\spark directory and copy all the contents of the file to that path.

Once the content has been copied, look in the C:\spark\conf directory for the log4j.properties.template file and rename it as log4j.properties. Then open the renamed file with a text editor and on the next line of the file

log4j.rootCategory=INFO, console

It should be changed to

log4j.rootCategory=ERROR, console

This is done so that when executing Spark commands in a terminal, it does not fill up with information that is not necessary. The following environment variable is created

SPARK_HOME = C:\spark

The %SPARK_HOME%\bin variable is added to the Path variable.

Finally, the apache-spark-sql-connector.jar file must be copied to the path C:/spark/jars in order to make connections to the SQL Server.

#### Winutils

You must create the C:\winutils\bin directory and copy the winutils.exe file to that path. Once the file has been copied, the following environment variable is created

HADOOP_HOME = C:\winutils

The %HADOOP_HOME%\bin variable is added to the Path variable.

A cmd terminal is opened and validated, execute the pyspark command, if everything works correctly, the Spark environment will start.

#### SQL Server

To install SQL Server, run the file SQLEXPRADV_x64_ENU.exe which was downloaded from the link, creating the 'sa' user that is the administrator of the server and which has all the permissions of the database.

Once the SQL Server is installed, you must open the SQL Server Management Studio and copy the contents of the Script_Database_Model.sql file located in the database directory.

The content must be executed in parts

The CREATE DATABASE script
The CREATE TABLES script
The CREATE VIEWS script

The other scripts in the file are to delete the tables and restart the sequences created, to be able to perform various load tests.

#### env file

It is necessary to create an environment file called .env in the path where the project will be.

CONN = com.microsoft.sqlserver.jdbc.spark
SQL_USER = sa
SQL_PWD = your_pwd_sa
SQL_DB = UBER_ANALISIS
SQL_SERVER = localhost
