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

TEST_DATASET=/user/adampap/WHVsmall


OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, A B C D)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runA: $(OBJDIR)/FilterRandomPercentage.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/A-output/
	hadoop jar $(JAR) FilterRandomPercentage -D percentage=1 $(TEST_DATASET) /$(HDFS)/A-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/A-output/part*"

runB: $(OBJDIR)/FindMostFrequentCombinations.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/B-output/
	hadoop jar $(JAR) FindMostFrequentCombinations -D N=10 $(TEST_DATASET) /$(HDFS)/B-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/B-output/part*"

runC: $(OBJDIR)/VisitsDataPerVisitor.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/C-output/
	hadoop jar $(JAR) VisitsDataPerVisitor  $(TEST_DATASET) /$(HDFS)/C-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/C-output/part*"

runD: $(OBJDIR)/HourStatistics.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/D-output/
	hadoop jar $(JAR) HourStatistics $(TEST_DATASET) /$(HDFS)/D-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/D-output/part*"

$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
