#! /bin/bash

if [ -z $1 ]
then
	echo 'You should specify an input folder....'
	exit $E_MISSING_POS_PARAM
fi

mkdir -p output/seg

for x in $( find $1 -name "*" );
do
	python ptos.py $x output/seg
done
