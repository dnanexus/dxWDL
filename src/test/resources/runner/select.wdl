workflow select {
    Boolean? flag

#    String x = if select_first([flag,false]) then 'OKAY' else 'FAIL'
    String y = if defined(flag) then 'OKAY' else 'FAIL'

    output {
#        String out_x = x
        String out_y = y
    }
}
