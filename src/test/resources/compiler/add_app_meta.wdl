version 1.0

task add {
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
            contactEmail: "joe@dev.com",
            upstreamVersion: "1.0",
            upstreamAuthor: "Joe Developer",
            upstreamUrl: "https://dev.com/joe",
            upstreamLicenses: ["MIT"],
            whatsNew: [
                { 
                    version: "1.1", 
                    changes: ["Added parameter --foo", "Added cowsay easter-egg"] 
                },
                {
                    version: "1.0", 
                    changes: ["Initial version"]
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
