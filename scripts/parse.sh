#! /bin/bash

mkdir -p output

if [ -z $1 ]
then
	echo 'You should specify an input folder....'
	exit $E_MISSING_POS_PARAM
fi

mkdir -p output/$1

for x in $( find $1 -name "*.gpx" );
do
	python gpx_parse.py $x output/$1
done
