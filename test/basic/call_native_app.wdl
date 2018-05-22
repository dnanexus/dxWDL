import "dx_app_extern.wdl" as lib

workflow call_native_app {
    File in_file
    File pack
#    call lib.swiss_army_knife as sak1 {
#        input: in= [in_file], cmd= "wc *"
#    }
#
#    if (true) {
#        call lib.swiss_army_knife as sak2 {
#            input: in= [in_file], cmd= "wc -l * > report.txt"
#        }
#    }
#
    call lib.swiss_army_knife as sak3 {
        input: in= [pack], cmd= "tar xvf *"
    }

    output {
#        Array[File] a = sak1.out
#        Array[File]? b = sak2.out
        Array[File] c = sak3.out
    }
}
