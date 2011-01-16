@echo off
set oldcd="%cd%"
cd /d %~dp0

rd /s /q output
python ../trunk/dupefind.py -c source1 > hash.csv
python ../trunk/dupefind.py -c source2 >> hash.csv
python ../trunk/dupefind.py --nodupe_copy hash.csv output

cd /d %cd%