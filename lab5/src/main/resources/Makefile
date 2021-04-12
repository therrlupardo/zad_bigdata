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

DATASET_URL=/user/adampap
SMALL_DATASET=$(DATASET_URL)/wikiSmall
SPIDER_TRAPS_DATASET=$(DATASET_URL)/wikiSpiderTraps
DEAD_ENDS_DATASET=$(DATASET_URL)/wikiDeadEnds

USED_DATASET=$(SPIDER_TRAPS_DATASET)

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, A B C)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runA: $(OBJDIR)/InitializedPageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/A-output/
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/tempA*
	hadoop jar $(JAR) InitializedPageRank -D N=4 -D K=10 -D B=0.8 -D tmpPath=/$(HDFS)/tempA $(SMALL_DATASET) /$(HDFS)/A-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/A-output/part*"

runB: $(OBJDIR)/InitializedPageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/B-output/
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/tempB*
	hadoop jar $(JAR) InitializedPageRank -D N=4 -D K=10 -D tmpPath=/$(HDFS)/tempB $(DEAD_ENDS_DATASET) /$(HDFS)/B-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/B-output/part*"

runC: $(OBJDIR)/InitializedPageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/C-output/
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/tempC*
	hadoop jar $(JAR) InitializedPageRank -D N=4 -D K=10 -D tmpPath=/$(HDFS)/tempC $(SPIDER_TRAPS_DATASET) /$(HDFS)/C-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/C-output/part*"

$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
