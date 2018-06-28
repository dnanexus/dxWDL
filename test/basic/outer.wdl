import "username.wdl" as username

workflow outer {
    String? who
    call username.username_wf { input: who = who }
}
