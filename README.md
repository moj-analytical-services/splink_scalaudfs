#  splink_scalaudfs

This module was started as an extension of an example provided in [1]
Phillip Lee (ONS) has created an example of a UDF defined in Scala, callable from PySpark,
that wraps a call to JaroWinklerDistance from Apache commons.


After understanding how this mechanism worked the intention was to add [more text distance and similarity metrics from Apache Commons](https://commons.apache.org/proper/commons-text/apidocs/org/apache/commons/text/similarity/package-summary.html) 
for use in fuzzy matching in Pyspark in [splink](https://github.com/moj-analytical-services/splink). 
Down the line some additional utility functions were added. More functions will be added for sure as time goes on ... 

Current plan is to make this package available through the maven/ivy repositories (think PyPI in Python) as `uk.gov.moj.dash.linkage`
so that it can be accessed more easily from AWS-Glue jobs





## Usage

To build the Jar (currently the Maven Build System is used):

    mvn package
    
To add the jar to PySpark set the following config:

    spark.driver.extraClassPath /path/to/jarfile.jar
    spark.jars /path/to/jarfile.jar
    
To register the functions with PySpark > 2.3.1:

```python

spark.udf.registerJavaFunction('jaro_winkler', 'uk.gov.moj.dash.linkage.JaroWinklerSimilarity',\ 
                                pyspark.sql.types.DoubleType())
                                
                                
spark.udf.registerJavaFunction('jaccard_sim', 'uk.gov.moj.dash.linkage.JaccardSimilarity',\ 
                                pyspark.sql.types.DoubleType())                          
                                
spark.udf.registerJavaFunction('cosine_distance', 'uk.gov.moj.dash.linkage.CosineDistance',\ 
                                pyspark.sql.types.DoubleType())
                                
.
.
.

from pyspark.sql import types as T
rt = T.ArrayType(T.StructType([T.StructField("_1",T.StringType()), 
                               T.StructField("_2",T.StringType())]))
                               
spark.udf.registerJavaFunction(name='DualArrayExplode', 
            javaClassName='uk.gov.moj.dash.linkage.DualArrayExplode', returnType=rt)
                                
                                
                                
```

---


###  JVM? How can I install this on my macbook?

Maven is written in Java (and primarily used for building JVM programs). 
* the major Maven prerequisite is a Java JDK. 

If you dont have one ,installing either OpenJDK or [AWS Correto JDK](https://aws.amazon.com/blogs/opensource/amazon-corretto-no-cost-distribution-openjdk-long-term-support/) is recommended.Oracle JDK is not anymore.


---


###  Scala? How can I install this on my macbook?


#### Install sdkman

An easy way to install `Scala` on your macbook is to use `sdkman`

- open a new terminal and run 

`curl -s "https://get.sdkman.io" | bash`

and follow the instructions on screen

- close the terminal. Open a new one and run 

`source "$HOME/.sdkman/bin/sdkman-init.sh"`
    
- run `sdk version` on the terminal in order to check your installation of sdkman was successful


if you use zsh ensure that these lines are the last lines of your .zshrc file. 



`#THIS MUST BE AT THE END OF THE FILE FOR SDKMAN TO WORK!!!
export SDKMAN_DIR="/Users/MYUSERNAME/.sdkman"
[[ -s "/Users/MYUSERNAME/.sdkman/bin/sdkman-init.sh" ]] && source "/Users/MYUSERNAME/.sdkman/bin/sdkman-init.sh"`


If they are not make them so!


#### Install Scala via sdkman

It is important to install a version of Scala that is in the 2.11 to 2.12 range as these are the ones compatible with Spark 2.4.5


Now that you have `sdkman` installed run 

`sdk install scala 2.12.10`




voila. you have Scala now on your machine.


Run `scala -version` and `scalac -version` to check


---


###  Maven? How can I install this on my macbook?


Manual way:


* To install Maven on Mac OS X operating system, download the latest version from the Apache Maven site, select the Maven binary tar.gz file, for example: apache-maven-3.3.9-bin.tar.gz to to Downloads/ 

    * Extract the archive with `tar -xzfv apache-maven-3.3.9-bin.tar.gz`

    * On the shell prompt: `sudo chown -R root:wheel Downloads/apache-maven*` for fixing permissions.

    * On the shell prompt: `sudo mv Downloads/apache-maven* /opt/apache-maven`

    * Edit your .bashprofile with `nano ~/.bash_profile` and add  `export PATH=$PATH:/opt/apache-maven/bin` there. Similerly add  this to your .zshrc file if you use zsh
    
    
* To install Maven on a Debian based Linux distribution (such as Ubuntu) there is an easier way: `sudo apt-get install maven`
 
* Test that everything has been installed fine by running `java -version` and `mvn -version`  on your bash prompt


sdkman way:


`sdk install maven`


lol. sdkman is quite useful isnt it?

---


###  Ok everything installed. How can I build a jar file to use with Pyspark?

* Go to the root folder of this repo (where the .pom file is)

* Then run `mvn package`


---




## Progress


v.0.0.7

* Added DualArrayExplode UDF . Also added Logit and Expit UDFs (experimental). Added alternate encoding of Double Metaphone from Apache Commons 


v.0.0.6

* Added QgramTokenisers for Q3grams,Q4grams,Q5grams,Q6grams 

v.0.0.5

* Added a small QgramTokeniser 

v.0.0.4

* Added Double Metaphone from Apache Commons  ( org.apache.commons.codec.language._ )


v.0.0.3

* cleaning up and housekeeping

v.0.0.2

* JaroWinklerSimilarity has been used instead of JaroWinklerDistance 
* Added CosineDistance and JaccardSimilarity from Apache Commons

v.0.0.1

* get this mechanism working and output JaroWinklerDistance jar. Test that its working on AP.



---





## References:

[1] [Using a Scala UDF Example](https://github.com/ONSBigData/scala_udf_example)  @philip-lee-ons



