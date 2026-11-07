"""Bazel rules for protovcs layouts.

A layout is checked when it is built, not when a store is first opened.
`protovcs_layout` runs `//cmd/protovcsgen`, which compiles the layout
against its message descriptor — compiling is the validation — and writes the
scope constructors and item key helpers a caller uses instead of composing
those by hand.
"""

load("@rules_go//go:def.bzl", "go_library")
load("@protobuf//bazel/common:proto_info.bzl", "ProtoInfo")

def _descriptor_sets(ctx):
    """Every descriptor the generator needs to resolve the layout's message.

    The generator does not link in the messages of every layout it may be asked
    about, so their descriptors arrive as data.
    """
    sets = [ctx.attr._layout_proto[ProtoInfo].transitive_descriptor_sets]
    for dep in ctx.attr.protos + ctx.attr.marker_protos:
        sets.append(dep[ProtoInfo].transitive_descriptor_sets)
    return depset(transitive = sets)

def _protovcs_layout_impl(ctx):
    go_out = ctx.actions.declare_file(ctx.label.name + ".pb.layout.go")
    descriptor_sets = _descriptor_sets(ctx)

    args = ctx.actions.args()
    args.add("--layout", ctx.file.src)
    args.add("--go_package", ctx.attr.go_package)
    args.add("--go_out", go_out)
    args.add_all(descriptor_sets, before_each = "--descriptor_set")
    args.add_all(ctx.files.locks, before_each = "--lock")
    args.add_all(ctx.attr.refuse_marked, before_each = "--refuse_marked")

    ctx.actions.run(
        outputs = [go_out],
        inputs = depset([ctx.file.src] + ctx.files.locks, transitive = [descriptor_sets]),
        executable = ctx.executable._generator,
        arguments = [args],
        mnemonic = "ProtovcsLayout",
        progress_message = "Compiling protovcs layout %s" % ctx.file.src.short_path,
    )

    return [DefaultInfo(files = depset([go_out]))]

protovcs_layout = rule(
    implementation = _protovcs_layout_impl,
    doc = "Compiles a protovcs layout and generates its accessors.",
    attrs = {
        "src": attr.label(
            allow_single_file = [".textproto"],
            mandatory = True,
            doc = "The layout to read.",
        ),
        "protos": attr.label_list(
            providers = [ProtoInfo],
            doc = "The proto_library target declaring the message the layout decomposes.",
        ),
        "go_package": attr.string(
            mandatory = True,
            doc = "Package clause of the generated Go file.",
        ),
        "locks": attr.label_list(
            allow_files = [".lock"],
            mandatory = True,
            doc = """The schema locks covering every message the layout can reach.

Mandatory, and the generator checks coverage rather than presence. A chunk is
named by a fingerprint of its canonical bytes, so a renumbered field changes
what every stored byte means and renames every chunk holding it. stableproto
refuses that, but only for the files it was told to lock, so a reachable message
nobody locked is the hole this closes.""",
        ),
        "refuse_marked": attr.string_list(
            doc = """Full names of bool message options that mark a message as never to be stored, such as a raw key.

The build fails if the layout's message can reach a message with one of them set.""",
        ),
        "marker_protos": attr.label_list(
            providers = [ProtoInfo],
            doc = "The proto_library targets declaring the options in refuse_marked.",
        ),
        "_layout_proto": attr.label(
            default = Label("//proto/protovcs/v1:protovcs_proto"),
            providers = [ProtoInfo],
        ),
        "_generator": attr.label(
            default = Label("//cmd/protovcsgen"),
            executable = True,
            cfg = "exec",
        ),
    },
)

def protovcs_layout_go(name, src, protos, locks, go_package, importpath, srcs = [], deps = [], refuse_marked = [], marker_protos = [], visibility = None):
    """A layout and the go_library of its accessors.

    `name` is the library a caller depends on. deps names the go_library of the
    message the layout decomposes, which the generated code refers to.

    `srcs` are hand-written files joining the generated ones in the same Go
    package, for what a store's callers need and a layout cannot state: the name
    of the branch they read, a helper that reaches it. Keeping them here rather
    than in a wrapper package means a caller imports one thing and cannot tell
    which half of it was generated, which is the point.

    `refuse_marked` and `marker_protos` name options that mark a message as never
    to be stored; see the protovcs_layout rule.

    A file here may not redeclare a generated name. The generated ones are
    listed in //cmd/protovcsgen, and the Go compiler reports a collision as
    a redeclaration in this package.
    """
    protovcs_layout(
        name = name + "_gen",
        src = src,
        protos = protos,
        locks = locks,
        go_package = go_package,
        refuse_marked = refuse_marked,
        marker_protos = marker_protos,
        visibility = ["//visibility:private"],
    )

    go_library(
        name = name,
        srcs = [":" + name + "_gen"] + srcs,
        importpath = importpath,
        visibility = visibility,
        deps = deps + [
            "//:protovcs",
            "//proto/protovcs/v1",
            "@org_golang_google_protobuf//encoding/prototext",
            "@org_golang_google_protobuf//proto",
            "@org_golang_google_protobuf//reflect/protoreflect",
        ],
    )
