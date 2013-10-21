Welcome to the P-Analytics Project from Cloudera!
=================================================

The P stands for {Profile,Pivoted,Parametrized,Predictive} analytics (choose whatever you feel comfortable with).  The project is about entity-based computations with non-structured changing data.  One example is session- or cookie-based computations in advertizing, but it can be anything entity-based like fraud detection, gaming, hardware fault analyisis, conversion funnel analysis, etc.

# Build

## Download and Compile Crunch

Download crunch from [http://crunch.apache.org]

Crunch needs to be compiled with the crunch.platform=2 flag to properly run in the mr1 mode on Hadoop

```bash
> mvn install -Dcrunch.platform=2 -DskipTests
```

## Building the Project

```bash
> mvn clean package -DskipTests -P DEPS,JOB
```

will build all target jars.

As an option, you can create the DEP and JOB files in the root directory to avoid typing the `-P DEPS,JOB` each time

```bash
touch DEP JOB
```

It's a good idea sometimes to set the env variable `export MAVEN_OPTS=-DskipTests` to save time unless you are working on a significant feature

## Running from the Commmand Line

Use the p-analytics.jar for Hive and Pig.  Some libraries (like, again, Avro) need to have dependenciesin the same jar, so the p-analytics-jar-with-dependencies.jar should be used in this case. Use the p-analytics-job.jar to run Crunch jobs from a command line:

```bash
> hadoop jar target/p-analytics-job.jar command input(s) output
```

For example, to run the conversion to Avro that can be loaded into Hive or Pig:

```bash
> hadoop jar target/p-analytics-job.jar avro data/hd/attr.txt data/hd/event.txt <output-dir>
```

To add compression (or add any other flag), you may do:

```bash
> hadoop jar target/p-analytics-job.jar avro -Dmapred.output.compress=true data/hd/attr.txt data/hd/event.txt <output-dir>
```

## Generating JavaDoc

```bash
$ mvn javadoc:javadoc
```

The javadocs will be in `target/site/apidocs/index.html`.

## Classpath

Sometimes the executable requires additional libraries that are not on the default Hadoop set.  To generate the classpath for all dependencies do

```mvn
> mvn -f pom.xml dependency:build-classpath
```

which needed to be added to `HADOOP_CLASSPATH` 

Look at src/main/{pig,hive} directories for the Pig and Hive scripts.  Pig and Hive need to be installed separately.

## Packaging

To build and package the sources and data into one zipped file, run:

```bash
> mvn clean assembly:assembly -DskipTests -P DEPS,JOB
```

# Maven and Eclipse

Read Maven "Getting Started" http://maven.apache.org/guides/getting-started/index.html.

## Creating a new project from scratch

```bash
> mvn archetype:generate -DgroupId=com.cloudera.fts -DartifactId=P-Analytics -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
> mvn -Declipse.workspace=<path-to-eclipse-workspace> eclipse:configure-workspace
> mvn -DdownloadSources=true -DdownloadJavadocs=true eclipse:clean eclipse:eclipse
```

Modify pom.xml (or edit dependencies later using Eclipse if you trust it)

To install the project in the local repo

```bash
> mvn install -DskipTests
```

## Eclipse integration (install m2e Eclipse plugin http://eclipse.org/m2e/download/)

This will setup your eclipse environment:

```bash
> mvn -DdownloadSources=true -DdownloadJavadocs=true eclipse:clean eclipse:eclipse
```

It will download a lot of data, so have a fast Internet connection when doing it for the first time.  Then, in Eclipse do the following:

1. File->Import...
2. General->Existing projects into workspace
3. select the "Next" button
4. select the projcet toplevel directory
5. select the "Finish" button

Each time you modify pom.xml outside of Eclipse you need to 'Update Maven Project' from the Eclipse project menu

## Generating Sources from Protobuf Definition Files and Avro Schema

To generate Protobuf and Avro code (from the Protobuf definition files are in the `src/main/proto` directory and the the Avro schema file in the `src/main/avro` directory), run:

```bash
> mvn generate-sources
```

Alternatively you can create and execute it as an eclipse target within Eclipse.  The generate java code can be found in target/generated-sources.  You might need to add the directory to the Eclipse Java build path by (an ecliplse plugin _paranamer-maven-plugin_ usually does it for you though) :

1. Go to Project Explorer view
2. Right click on a project
3. Go to "Build Path" -> "Configure Build Path..." -> "Add Folder..."
4. add target/generate-sources folder to the build path

You alse need to add the _conf_ directory and the _p-analytics-job.jar_ to the classpath if you want to run the executable from within Eclipse.

## Dependency Tree

To analyze maven dependencies, run

```bash
> mvn dependency:tree -Dverbose
```

Avoid multiple versions of the same jar: Some versions of the libraries might have conflicts (like Avro)
