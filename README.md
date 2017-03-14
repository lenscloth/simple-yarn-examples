# About

This repository includes some simple yarn examples written in scala 

It contains basic flow of basic yarn application
    
    1) Submit a job to yarn cluster using YarnClient
    2) Deploy a ApplicationMaster
    3) Deploy a new container using ApplicationMaster
    
This repository is under development

# Usage

```
sbt package // simpleyarn-2.11.jar
export HADOOP_CLASSPATH="scala-2.11-lib.jar:scopt_2.11-3.5.0.jar:$HADOOP_CLASSPATH" 
// simple-yarn-exampes has dependendy on scala lib and scopt (https://github.com/scopt/scopt)

// print usage
yarn jar simpleyarn_2.11-1.0.jar org.lenscloth.hadoop.yarn.examples.run

// general usage
yarn simpleyarn_2.11-1.0.jar org.lenscloth.hadoop.yarn.examples.run 
    -name name_of_applicaiton 
    -s staging_directory_in_hdfs 
    -c first_deployed_container_commnand 
    -r first_deployed_container_resources
```

# Examples
## Spreader
    
Spreader will spread command to n container.

All the containers will have same resources and run the same command

```
java org.lenscloth.hadoop.yarn.examples.component.Spreader {number of container to spread command} {commands to run}
```
```
yarn jar simpleyarn_2.11-1.0.jar org.lenscloth.hadoop.yarn.examples.run 
   --name spreader -s /user/wppark/ 
   -r scala-library-2.11.8.jar,simpleyarn_2.11-1.0.jar,scopt_2.11-3.5.0.jar 
   -c "$JAVA_HOME/bin/java org.lenscloth.hadoop.yarn.examples.component.Spreader 10 'echo Hello World'"
```
The command above will launch 10 container inside yarn cluster that will launch ```echo 'Hello World'``` command
