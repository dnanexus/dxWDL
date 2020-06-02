version 1.0

# a workflow struct definitions

struct House {
    Int height
    Int num_floors
    String street
    String city
}

workflow foo {
    input {
    }

    House a = object {
      height: 20,
      num_floors : 3,
      street : "Alda",
      city : "Sunnyvale"
    }
    House b = object {
      height: 12,
      num_floors : 1,
      street : "Mary",
      city : "Santa Clara"
    }

    Int tot_height = a.height + b.height
    Int tot_num_floors = a.num_floors + b.num_floors
    String streets = a.street + "_" + b.street
    String cities = a.city + "_" + b.city

    House tot = object {
      height: tot_height,
      num_floors : tot_num_floors,
      street : streets,
      city : cities
    }

    output {
        House result = tot
    }
}
