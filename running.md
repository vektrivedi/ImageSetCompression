Prerequisites:
•	Numpy :  http://www.numpy.org/
•	Scipy: http://www.scipy.org/
•	thundar: http://thunder-project.org/thunder/docs/install_local.html
•	PIL : http://www.pythonware.com/products/pil/

Step:1 
	Create "ImageSetCompression" folder/directory on local server. 
Step:2
	Get the "Input" directory from github (https://github.com/vivek-trivedi/ImageSetCompression/tree/master/input) into "/ImageSetCompression". DONOT USE HDFS AS THUNDER DOESN'T READ PROPERLY FROM It.

Step:3 (local)

Local
-----

For Compression
>> spark-submit --master local ImageSetCompression.py c /vivek/ImageSetCompression/input /vivek/ImageSetCompression/output

For DeCompression
>> spark-submit --master local ImageSetCompression.py d /vivek/ImageSetCompression/output


Cluster
--------
>> module load bigdata
>> module load spark/1.5.2
>> pyspark --master yarn-client

For Compression
>> spark-submit --master yarn-cluster ImageSetCompression.py c /vivek/ImageSetCompression/input /vivek/ImageSetCompression/output

For DeCompression
>> spark-submit --master yarn-cluster ImageSetCompression.py d /vivek/ImageSetCompression/output
