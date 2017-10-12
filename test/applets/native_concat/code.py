#!/usr/bin/env python

@dxpy.entry_point('main')
def main(**job_inputs):
    a = ""
    if 'a' in job_inputs:
        a = job_inputs['a']
    b = ""
    if 'b' in job_inputs:
        b = job_inputs['b']

    # Return output
    output = {}
    output["c"] = a + b
    return output

dxpy.run()
