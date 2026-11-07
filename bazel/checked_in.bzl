"""Keeps a copy of generated Go code in the source tree.

Bazel builds never read the copies; they use the generated originals. The
copies are there so that `go get` and `go test ./...` work without Bazel.
`<name>` is a test that fails when a copy is stale, and `bazel run <name>`
updates it.
"""

load("@bazel_lib//lib:write_source_files.bzl", "write_source_files")

def checked_in_go(name, src, files, output_group = None):
    """Checks in generated Go files.

    Args:
      name: the write_source_files target.
      src: the target generating the files.
      files: a dict from each checked-in path, relative to this package, to the
        basename of the generated file it copies.
      output_group: the output group of src holding the files, such as
        "go_generated_srcs" for a go_proto_library.
    """
    native.filegroup(
        name = name + "_generated",
        srcs = [src],
        output_group = output_group,
        visibility = ["//visibility:private"],
    )
    copies = {}
    for dest, base in files.items():
        rule = name + "_" + dest.replace("/", "_").replace(".", "_")
        native.genrule(
            name = rule,
            srcs = [":" + name + "_generated"],
            outs = [rule + ".go"],
            cmd = "for f in $(SRCS); do case \"$$f\" in */%s) cp \"$$f\" $@;; esac; done; test -s $@" % base,
            visibility = ["//visibility:private"],
        )
        copies[dest] = ":" + rule
    write_source_files(name = name, files = copies)
