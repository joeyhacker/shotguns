# Shotguns
It's a framework help you deploy distribution application on yarn

## Running 

1. make sure you have a set of hadoop cluster

2. write a job description file,  example: 
```json
{
  "appName": "app-test",
  "appJar": "/path/to/your.jar",
  "mainClass": "this.is.main.Class",
  "dependencies": [
    "/path/to/your/libs/dir"
  ],
  "environment": {
    "foo": "bar"
  },
  "containers": 2,
  "requirement": {
    "memory": 256,
    "virtualCores": 1
  }
}
```

3. java -DHADOOP_CONF_DIR=/path/to/hadoop_conf_dir -classpath $CLASSPATH com.joey.shotguns.AppSubmitter -f /path/to/job.json



