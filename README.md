# Flink Chasing Project

This project is based on Flink 1.17 version, aiming at walking through the Flink functionality. As a newer for Flink 
framework, you could easily pick it up through the project. The codes might be modified or customized by you to be suitable
for production environments. As a developer of Flink framework, you could reference the project to view the usage of 
common APIs.

## About Apache Flink

Apache Flink is an open-source, unified stream-processing and batch-processing framework developed by the Apache 
Software Foundation. It is a robust, scalable, and efficient tool that allows the processing of data streams.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing 
purposes.

### Prerequisites

What things you need to install the software and how to install them.

```bash
# java 11 preferable 
java -version
maven -version
```

### Installing

A step-by-step series of examples that tell you how to get a development environment running.

```
# Clone the repository
git clone https://github.com/ddsr-ops/flink-1.17-chase.git

# Navigate to the project directory
cd projectname

# Build the project with Maven
mvn clean package
```

### Deployment

Add additional notes about how to deploy this on a live system.

```
# Example deployment steps
flink run -c com.yourorg.MainClass your_jar_file.jar
```

# License

This project is licensed under the [MIT License] License - see the LICENSE.txt file for details