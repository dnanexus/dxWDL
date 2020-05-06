version 1.0

workflow foo {
  # convert an optional file-array to a file-array
  Array[File] files = ["a", "b"]
  Array[File]? files2 = files

  # Convert an optional int to an int
  # This will fail at runtime if a is null
  Int a = 3
  Int? b = a

  # An array with a mixture of Int and Int?
  Int c = select_first([1, b])

  # Convert an integer to a string
  String s = 3

  # https://github.com/gatk-workflows/gatk4-germline-snps-indels/blob/master/JointGenotyping-terra.wdl#L475
  # converting Array[String] to Array[Int]
  Int d = "3"
  Array[Int] numbers = ["1", "2"]

  # https://github.com/gatk-workflows/gatk4-germline-snps-indels/blob/master/JointGenotyping-terra.wdl#L556
  #
  # Branches of a conditional may require unification
  Array[String] birds = ["finch", "hawk"]
  Array[String?] mammals = []
  Array[String?] zoo = if (true) then birds else mammals

  # https://github.com/gatk-workflows/gatk4-germline-snps-indels/blob/master/haplotypecaller-gvcf-gatk4.wdl#L211
  Int z = 1.3

  # https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/master/JointGenotypingWf.wdl#L570
  #
  # Adding a String and String?
  String? here = "here"
  String buf = "not " + "invented " + here

  # https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/master/PairedEndSingleSampleWf.wdl#L1207
  #
  # convert an optional int to string.
  Int? max_output = 3
  String s1 = "MAX_OUTPUT=" + max_output

  # Check we can support place holders outside a command block
  String? ignore = "xx"
  String s2 = "~{default='null' ignore}"

  # https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/master/PairedEndSingleSampleWf.wdl#L1208
#  Array[String]? ignore
#  String s2 = {default="null" sep=" IGNORE=" ignore}

  # syntax error in place-holder
  # https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/master/PairedEndSingleSampleWf.wdl#L1210
#  Boolean? is_outlier_data
#  String s3 = ${default='SKIP_MATE_VALIDATION=false' true='SKIP_MATE_VALIDATION=true' false='SKIP_MATE_VALIDATION=false' is_outlier_data}

  # Uses the keyword 'version' as a field returned from a task
  # https://github.com/gatk-workflows/gatk4-data-processing/blob/master/processing-for-variant-discovery-gatk4.wdl#L115
}
