#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
export SPARK_HOME=/usr/lib/spark
export PYSPARK_PYTHON=/home/hadoop/agente/venv_agente/bin/python
export PYSPARK_DRIVER_PYTHON=/home/hadoop/agente/venv_agente/bin/python
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

python agentecondash.py