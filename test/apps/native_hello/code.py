#!/usr/bin/env python

@dxpy.entry_point('main')
def main(**job_inputs):
    who = job_inputs['who']

    # Return output
    greeting = "hello {}".format(who)
    output = {}
    output["greeting"] = greeting
    return output

dxpy.run()
