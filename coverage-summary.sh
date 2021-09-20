#!/bin/bash

##
## jacoco-summary.sh
## Parse jacoco.csv and print a summary (similar to phpunit) on stdout
## https://cylab.be/blog/94/compute-the-code-coverage-of-your-tests-with-java-and-maven
## requires gawk (GUN awk)
##

if [ "$#" -ne 1 ]; then
  echo "Print a summary of jacoco code coverage analysis."
  echo "Usage: $0 <path/to/jacoco.csv>";
  exit 1;
fi

awk -F ',' '{
  inst += $4 + $5;
  inst_covered += $5;
  br += $6 + $7;
  br_covered += $7;
  line += $8 + $9;
  line_covered += $9;
  comp += $10 + $11;
  comp_covered += $11;
  meth += $12 + $13;
  meth_covered += $13; }
END {
  print "Code Coverage Summary:";
  printf "  Instructions: %.2f% (%d/%d)\n", 100*inst_covered/inst, inst_covered, inst;
  printf "  Branches:     %.2f% (%d/%d)\n", 100*br_covered/br, br_covered, br;
  printf "  Lines:        %.2f% (%d/%d)\n", 100*line_covered/line, line_covered, line;
  printf "  Complexity:   %.2f% (%d/%d)\n", 100*comp_covered/comp, comp_covered, comp;
  printf "  Methods:      %.2f% (%d/%d)\n", 100*meth_covered/meth, meth_covered, meth; }
' $1
