workflow test {
  File gatk4_happy_summary_csv
  File strelka2_happy_summary_csv
  File pbgermline_happy_summary_csv
  File? gatk4_happy_stratified_extended_csv
  File? strelka2_happy_stratified_extended_csv
  File? pbgermline_happy_stratified_extended_csv
  String sample
  call merge_happy_summaries2 {
    input: happy_summary_csvs = [ gatk4_happy_summary_csv,
                                strelka2_happy_summary_csv,
                                pbgermline_happy_summary_csv ],
      happy_stratified_extended_csvs = select_all([ gatk4_happy_stratified_extended_csv,
                                                  strelka2_happy_stratified_extended_csv,
                                                  pbgermline_happy_stratified_extended_csv ]),
      sample = sample,
  }
}

task merge_happy_summaries2 {
  Array[File]+ happy_summary_csvs
  Array[File] happy_stratified_extended_csvs
  String sample

  command <<<
    mkdir -p ${sample}_merged_happy_summary
    for file in ${sep=' ' happy_summary_csvs}  ; do
    output_filename=$(basename $file _happy.summary.csv)
    echo $output_filename >> ${sample}_merged_happy_summary.csv
    cat $file >> ${sample}_merged_happy_summary.csv

    mv $file ${sample}_merged_happy_summary
    done
    tar -zcvf ${sample}_merged_happy_summary.tar.gz ${sample}_merged_happy_summary
    while read file; do
    output_filename=$(basename $file _happy.extended.csv)
    echo $output_filename >> ${sample}_merged_happy_extended.csv
    cat $file >> ${sample}_merged_happy_extended.csv

    mv $file ${sample}_merged_happy_extended
    done < ~{write_lines(happy_stratified_extended_csvs)}
    tar -zcvf ${sample}_merged_happy_extended.tar.gz ${sample}_merged_happy_extended
    cat ${sep=' ' happy_stratified_extended_csvs}
  >>>
  output {
    File? merged_extended_csv = "${sample}_merged_happy_extended.csv"
    File? merged_extended_targz = "${sample}_merged_happy_extended.tar.gz"
    File merged_summary_csv = "${sample}_merged_happy_summary.csv"
    File merged_summary_targz = "${sample}_merged_happy_summary.tar.gz"
  }
}
