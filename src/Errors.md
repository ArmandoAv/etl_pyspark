# Common Errors

In this file some common errors will be listed and how they can be fixed.

## Transform.py

Error:
py4j.protocol.Py4JJavaError: An error occurred while calling o90.csv.
: org.apache.spark.sql.AnalysisException: Path does not exist: file:/...../etl_pyspark/output/uber_data_yyyymmdd.csv;

Cause:
This error is caused beause doesn't exist the file uber_data_yyyymmdd.csv

Possible solution:
To correct it you must to run the python job Extract.py and then you can run the job Transform.py. If you run the python job Extract.py one day and you run the pyhton job Transform.py another the process fail, because the files names created have a current date.

```
spark-submit Extract.py
spark-submit Transform.py
```

Error:
The file "fileName_yyyymmdd.csv" has been crated in output path without records.
Please check because there aren't records in the file

Cause:
Sometimes the python job Transform.py can be created a file without records.

Possible solution:
To correct it you must run the python job Transform.py again

```
spark-submit Transform.py
```

## Load.py

Error:
py4j.protocol.Py4JJavaError: An error occurred while calling o56.save. : com.microsoft.sqlserver.jdbc.SQLServerException: No se pudo realizar la conexi¾n TCP/IP al host localhost, puerto 1433. Error: "Connection refused: connect. Verifique las propiedades de conexi¾n, compruebe que hay una instancia de SQL Server ejecutßndose en el host y aceptando las conexiones TCP/IP en el puerto y compruebe que no hay ning·n firewall bloqueando las conexiones TCP en el puerto.".

Cause:
This error is caused by the database server being down.

Possible solution:
To correct it you must open the SQL Server Management Studio and activate the server. If the errror continues, you shpuld check in the SQL Server Configuration Manager that TCP/IP connections are enabled and within TCP/IP in the IP Adresses tab in the TCP Port options you have the number of port 1433.

Error:
The file fact_final_pago_viaje_yyyymmdd.bad has been crated in bad path wit "n" records.
Please check because there are records in the bad file.

Cause:
This error is caused by the final files that had been created since when loading these files into their tables, an ID is generated to each record and when the final file of the table is created FACT_PAGO_VIAJE takes the IDs created as part of its columns.

Possible solution:
To correct it, you must run the python Load.py job again, so that the table IDs can be generated and the error is fixed.

```
spark-submit Load.py
```

Warning:
File "C:\spark\python\lib\py4j-0.10.7-src.zip\py4j\java_gateway.py", line 929, in \_get_connection
connection = self.deque.pop()
IndexError: pop from an empty deque

Cause:
This warning occurs when it try to delete a connection that does not exist the process corrects it when it have a connection again, however it sends this warning before continuing with the loads of the tables

## Contributing

If you find any new bugs and their possible fix, I hope you can help by contributing this will be **greatly appreciated**.
