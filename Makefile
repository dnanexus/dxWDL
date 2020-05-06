ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

all : download_antlr_jar draft2 v1

download_antlr_jar:
	(if [ ! -e ${ROOT_DIR}/antlr-4.8-complete.jar ]; then \
		wget https://www.antlr.org/download/antlr-4.8-complete.jar; \
	fi)

draft2 :
	(cd ${ROOT_DIR}/src/main/antlr4/draft_2;  java -jar ${ROOT_DIR}/antlr-4.8-complete.jar -o ${ROOT_DIR}/src/main/java -visitor -package org.openwdl.wdl.parser.draft_2 WdlDraft2Parser.g4 WdlDraft2Lexer.g4)

v1 :
	(cd ${ROOT_DIR}/src/main/antlr4/v1; java -jar ${ROOT_DIR}/antlr-4.8-complete.jar -o ${ROOT_DIR}/src/main/java -visitor -package org.openwdl.wdl.parser.v1 WdlV1Parser.g4 WdlV1Lexer.g4)


clean :
	rm -rf src/main/java/*Lexer*
	rm -rf src/main/java/*Parser*
