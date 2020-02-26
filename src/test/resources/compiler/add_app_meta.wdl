version 1.0

task add_app_meta {
    input {
        Int a
        Int b
    }

    meta {
        title: "Add Ints"
        summary: "Adds two int together"
        description: "This app adds together two integers and returns the sum"
        developer_notes: "Check out my sick bash expression! Three dolla signs!!!"
        version: "1.0"
        open_source: true
        details: {
            contact_email: "joe@dev.com",
            upstream_version: "1.0",
            upstream_author: "Joe Developer",
            upstream_url: "https://dev.com/joe",
            upstream_licenses: ["MIT"],
            change_log: [
                { 
                    version: "1.1", 
                    changes: ["Added paramter --foo", "Added cowsay easter-egg"] 
                },
                {
                    version: "1.0", 
                    changes: ["Intial version"]
                }
            ]
        }
    }
    
    command {
        echo $((${a} + ${b}))
    }

    output {
        Int result = read_int(stdout())
    }
}
