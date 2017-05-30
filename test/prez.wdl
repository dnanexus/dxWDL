# Task converting FASTQ to FASTA
task FastqToFastA {
  String basename
  File iFile

  command <<<
python <<CODE
import sys
import itertools

cycleindicator = itertools.cycle('1234')
ifs = open("${iFile}", 'r')
ofs = open("${basename}.fasta", 'w')
for line in ifs:
    cycleindicator_current = cycleindicator.next()
    if cycleindicator_current == '1':
        ofs.write('>'+line.strip()[1:])
    elif cycleindicator_current == '2':
        ofs.write(line.strip())
    else:
        pass
CODE
>>>
  output {
     File out = "${basename}.fasta"
  }
}

# Workflow takes a FASTQ file, calculates
# basename, and calls task.
workflow prez {
    File fastq
    call FastqToFastA {
       input:
         basename = sub(fastq, ".fastq", ""),
         iFile=fastq,
    }
    output { FastqToFastA.out }
}