# construct a map that uses an array of files
task createMultiFruit {
    command <<<
      echo "Pear" > P.txt
      echo "Tomato" > T.txt
      echo "Cherry" > C.txt
      echo "Carrots" > C2.txt
    >>>
    output {
        Map[String, Array[File]] m = {"P": ["P.txt"], "T": ["T.txt"], "C": ["C.txt", "C2.txt"]}
    }
}

# Add to complex numbers represented as objects
task ComplexAdd {
    Object y
    Object z

    command <<<
python <<CODE
a = int(${y.a}) + int(${z.a})
b = int(${y.b}) + int(${z.b})
print('\t'.join(["a","b"]))
print('\t'.join([str(a), str(b)]))
CODE
>>>
    output {
        Object result = read_object(stdout())
    }
}

workflow dict {
    call createMultiFruit

    output {
        Map[String, Array[File]] results = createMultiFruit.m
    }
}
