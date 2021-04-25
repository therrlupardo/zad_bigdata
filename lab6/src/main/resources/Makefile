#
# This is a simple makefile to assist with quickly building the Exercises of MP2.
#
# To build and execute a particular exercise:
#    - For a single exercise, type 'make runA' to run exercise A.
#    - For all exercises, 'make'.
#
#
HADOOP_CLASSPATH := ${JAVA_HOME}/lib/tools.jar
export HADOOP_CLASSPATH

HDFS=user/bigdata39/psso

DATASET_URL=/user/adampap/extendedStackOverflow

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, A)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runA: $(OBJDIR)/DriverStackOverflow.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/A-output/
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/tempA*
	hadoop jar $(JAR) DriverStackOverflow -D tmpPath=/$(HDFS)/tempA $(DATASET_URL) /$(HDFS)/A-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/A-output/part*"

$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
