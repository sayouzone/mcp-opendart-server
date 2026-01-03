#!/bin/bash

TARGET_PATH=~/Development/sayouzone/sayou-fabric/packages-dev/sayou-stock
SRC_PATH=$TARGET_PATH/src/sayou/stock

cp ./tests/*.py $TARGET_PATH/tests/

if [ -f ./tests/corpcode.json ]; then
    cp ./tests/corpcode.json $TARGET_PATH/tests/
fi

cp ./tests/.env-samples $TARGET_PATH/tests/

cp -r ./edgar $SRC_PATH/
rm -rf $SRC_PATH/edgar/__pycache__/
rm -rf $SRC_PATH/edgar/parsers/__pycache__/
rm -rf $SRC_PATH/edgar/examples.py

cp -r ./opendart $SRC_PATH/
rm -rf $SRC_PATH/opendart/__pycache__/
rm -rf $SRC_PATH/opendart/parsers/__pycache__/
rm -rf $SRC_PATH/opendart/examples.py

cp -r ./yahoo $SRC_PATH/
rm -rf $SRC_PATH/yahoo/__pycache__/
rm -rf $SRC_PATH/yahoo/parsers/__pycache__/
rm -rf $SRC_PATH/yahoo/examples.py

cp -r ./naver $SRC_PATH/
rm -rf $SRC_PATH/naver/__pycache__/
rm -rf $SRC_PATH/naver/parsers/__pycache__/
rm -rf $SRC_PATH/naver/examples.py

cp -r ./fnguide $SRC_PATH/
rm -rf $SRC_PATH/fnguide/__pycache__/
rm -rf $SRC_PATH/fnguide/parsers/__pycache__/
rm -rf $SRC_PATH/fnguide/examples.py
