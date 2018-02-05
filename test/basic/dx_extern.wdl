# This file was generated by the Dx Native Interface (DxNI) tool.
# project name = dxWDL_playground
# project ID = project-F07pBj80ZvgfzQK28j35Gj54
# folder = /builds/2018-02-01/161022-0.58-14-g0399f79-dirty/applets

task native_concat {
    String? a 
    String? b 
    command {
    }
    output {
        String c = ""
    }
    meta {
        type: "native" 
        id: "applet-F9kppg00Zvgz6v41115Vx4G5" 
    }
}
task native_diff {
    File a 
    File b 
    command {
    }
    output {
        Boolean equality = true
    }
    meta {
        type: "native" 
        id: "applet-F9kppgQ0ZvgV8yXk11XP9vzg" 
    }
}
task native_mk_list {
    Int a 
    Int b 
    command {
    }
    output {
        Array[Int]+ all = [0]
    }
    meta {
        type: "native" 
        id: "applet-F9kppgj0ZvgV8yXk11XP9vzk" 
    }
}
task native_sum {
    Int? a 
    Int? b 
    command {
    }
    output {
        Int result = 0
    }
    meta {
        type: "native" 
        id: "applet-F9kppj00Zvgq710P112FP5XQ" 
    }
}
task native_sum_012 {
    Int? a 
    Int? b 
    command {
    }
    output {
        Int result = 0
    }
    meta {
        type: "native" 
        id: "applet-F9kppj80Zvgq710P112FP5XX" 
    }
}