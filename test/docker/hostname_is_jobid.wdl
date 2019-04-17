# show that hostname is actually job ID for easier debugging of jobs
# assert should look like hostname.startswith('job-')
task example {
    command <<<
    python <<'EOF'
import json
import os
import socket
import subprocess
hostname = socket.gethostname()
print('Found hostname %r' % hostname)
job_id = subprocess.check_output(['bash', '-c', 'source environment && echo $DX_JOB_ID']).strip()
if not job_id:
    raise ValueError('could not find job ID :(')
if hostname != job_id:
    with open('job_error.json', 'w') as fp:
        json.dump({"error": {"type": "AppError", "message": "Expected hostname to be job id. Got %r instead :(" % hostname}}, fp)
    raise ValueError('hostname %r did not match job id %r' % (hostname, job_id))
EOF
    >>>
    runtime {
        docker: "broadinstitute/genomes-in-the-cloud:2.2.4-1469632282"
    }
    output {
        String hostname = read_string(stdout())
    }
}
